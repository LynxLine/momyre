/*
“Commons Clause” License Condition v1.0

The Software is provided to you by the Licensor under the License, as defined
below, subject to the following condition.

Without limiting other conditions in the License, the grant of rights under the
License will not include, and the License does not grant to you,  right to Sell
the Software.

For purposes of the foregoing, “Sell” means practicing any or all of the rights
granted to you under the License to provide to third parties, for a fee or other
consideration (including without limitation fees for hosting or consulting/
support services related to the Software), a product or service whose value
derives, entirely or substantially, from the functionality of the Software.  Any
license notice or attribution required by the License must also include this
Commons Cause License Condition notice.

Software: momyre
License: MIT
Licensor: yshurik
*/

package app

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

var f_force = flag.Bool("force", false,
	"force to allow deletion of tables and columns which are not part of replication")

type sflag_t struct {
	set   bool
	value string
}

func (sf *sflag_t) Set(x string) error {
	sf.value = x
	sf.set = true
	return nil
}

func (sf *sflag_t) String() string {
	return sf.value
}

var f_mongo sflag_t
var f_mysql sflag_t

func init() {
	flag.Var(&f_mongo, "inp", "mongo connection url (example: mongodb://localhost:27017/test)")
	flag.Var(&f_mysql, "out", "mysql connection url (example: mysql://user:pass@localhost:3306/test)")
}

type Config struct {
	Cols_mo   map[string][]string
	Cols_my   map[string][]string
	Tables_mo map[string]map[string]string
	Tables_my map[string]map[string]string
	Inp       string                   `yaml:"inp"`
	Out       string                   `yaml:"out"`
	TablesYml map[string]yaml.MapSlice `yaml:"tables"`
}

var Conf Config

func Run() error {
	flag.Parse()
	log.Infoln("momyre started")
	{ // load config as first task
		buf, err := ioutil.ReadFile("momyre.yml")
		if err != nil {
			return fmt.Errorf("momyre.yml read: %v", err)
		}
		err = yaml.Unmarshal(buf, &Conf)
		if err != nil {
			return fmt.Errorf("momyre.yml parse: %v", err)
		}
		Conf.Cols_mo = make(map[string][]string)
		Conf.Cols_my = make(map[string][]string)
		Conf.Tables_mo = make(map[string]map[string]string)
		Conf.Tables_my = make(map[string]map[string]string)
		for table, m := range Conf.TablesYml {
			cols_mo := make([]string, 0)
			cols_my := make([]string, 0)
			Conf.Tables_mo[table] = make(map[string]string)
			Conf.Tables_my[table] = make(map[string]string)
			for _, mi := range m {
				col := mi.Key.(string)
				typ := mi.Value.(string)
				if col == "_id" {
					continue
				}
				cols_mo = append(cols_mo, col)
				cols_my = append(cols_my, col4sql(col))
				Conf.Tables_mo[table][col] = typ
				Conf.Tables_my[table][col4sql(col)] = typ
			}
			Conf.Cols_mo[table] = cols_mo
			Conf.Cols_my[table] = cols_my
		}
	}

	if f_mongo.set {
		Conf.Inp = f_mongo.value
	}
	if f_mysql.set {
		Conf.Out = f_mysql.value
	}
	u, _ := url.Parse(Conf.Out)
	if u.User != nil {
		usr := u.User.Username()
		u.User = url.User(usr)
	}

	log.Infoln("momyre use mongo:", Conf.Inp)
	log.Infoln("momyre use mysql:", u.String())
	log.Infoln("momyre use force:", *f_force)

	log.Infoln("momyre repl tables:", len(Conf.Tables_mo))
	modb, err := ConnectMo(Conf.Inp, 15*time.Second)
	if err != nil {
		return err
	}
	defer modb.client.Disconnect(context.Background())
	log.Infoln("momyre connected mongo db:", modb.dbname)

	mydb, err := ConnectMy(Conf.Out, 15*time.Second)
	if err != nil {
		return err
	}
	defer mydb.client.Close()
	log.Infoln("momyre connected mysql db:", mydb.dbname)

	is_from_scratch := false
	if mydb.Timestamp == 0 {
		is_from_scratch = true
	}

	// mysql sync config of dest tables
	resync_columns, err := mydb.SyncTablesConfig()
	if err != nil {
		return err
	}

	need_resync := false
	tables_resync := make([]string, 0)

	if is_from_scratch {
		// resync all tables and all columns
		need_resync = true
		log.Warnln("momyre resync tables from scratch timestamp=0")
		resync_columns = make(map[string]map[string]bool)
		for table, cols := range Conf.Tables_mo {
			tables_resync = append(tables_resync, table)
			resync_columns[table] = make(map[string]bool)
			for col, _ := range cols {
				resync_columns[table][col] = true
			}
		}
	} else {
		for table, cols := range resync_columns {
			if len(cols) > 0 {
				need_resync = true
				tables_resync = append(tables_resync, table)
			}
		}
	}

	if need_resync {
		log.Warnln("momyre need resync table due to conf changes:", tables_resync)

		err = modb.pauseWrites()
		if err != nil {
			return err
		}

		// read last timestamp from oplog
		// the log watch happens from this timestamp
		// all logs during tables resync can be reapplied
		tsnum, err := modb.readTimestamp()
		if err != nil {
			return err
		}
		log.Infoln("momyre mongo timestamp:", tsnum)

		for table, _ := range Conf.Tables_mo {

			cols := resync_columns[table]
			if len(cols) == 0 {
				continue
			}

			obj_ch := make(chan map[string]interface{}, 100)

			go modb.readTable(table, obj_ch)

			for {
				obj, opened := <-obj_ch
				if !opened {
					break
				}
				err := mydb.upsertRow(table, obj)
				if err != nil {
					return err
				}
			}
		}

		// update timestamp to mysql only if we are syncing from scratch
		// if not there are missing log entries while momyre was stopped/paused
		if is_from_scratch {
			err = mydb.updateTimestamp(tsnum)
			if err != nil {
				return err
			}
		}

		err = modb.unpauseWrites()
		if err != nil {
			return err
		}
	}

	// to run replication

	in_ch := make(chan map[string]interface{}, 100)
	up_ch := make(chan map[string]interface{}, 100)
	de_ch := make(chan map[string]interface{}, 100)

	go func(
		in_ch <-chan map[string]interface{},
		up_ch <-chan map[string]interface{},
		de_ch <-chan map[string]interface{}) {

		in_ok := true
		up_ok := true
		de_ok := true
		for in_ok || up_ok || de_ok {

			select {
			case obj, opened := <-in_ch:
				if !opened {
					in_ok = false
					break
				}
				table, _ := obj["$ns"].(string)
				err := mydb.upsertRow(table, obj)
				if err != nil {
					log.Errorln("momyre mysql insert row:", err)
					continue
				}
			case obj, opened := <-up_ch:
				if !opened {
					up_ok = false
					break
				}
				table, _ := obj["$ns"].(string)
				err := mydb.updateRow(table, obj)
				if err != nil {
					log.Errorln("momyre mysql update row:", err)
					continue
				}
			case obj, opened := <-de_ch:
				if !opened {
					de_ok = false
					break
				}
				table, _ := obj["$ns"].(string)
				idhex, _ := obj["_id"].(string)
				tsnum := uint64(0)
				if ts, has := obj["$ts"]; has {
					tsnum_tmp, is_uint64 := ts.(uint64)
					if is_uint64 {
						tsnum = tsnum_tmp
					} else {
						log.Infoln("momyre mysql $ts has wrong type")
					}
				}
				err := mydb.deleteRow(table, idhex, tsnum)
				if err != nil {
					log.Errorln("momyre mysql delete row:", err)
					continue
				}
			}
		}

		log.Warnln("momyre replication mysql loop finished...")

	}(in_ch, up_ch, de_ch)

	log.Warnln("momyre replication about to start...")
	tables := make([]string, 0, len(Conf.Tables_mo))
	for table, _ := range Conf.Tables_mo {
		tables = append(tables, table)
	}
	err = modb.Run(tables, mydb.Timestamp, in_ch, up_ch, de_ch)
	if err != nil {
		return err
	}
	log.Infoln("momyre finished")

	return nil
}
