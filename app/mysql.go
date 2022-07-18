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
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MysqlDB struct {
	dbname    string
	timeout   time.Duration
	client    *sql.DB
	Timestamp uint64
}

type ConfRec struct {
	Name  string
	Value []byte
}

func col4sql(x string) string {
	return strings.ReplaceAll(x, ".", "_")
}

func ConnectMy(conn string, timeout time.Duration) (*MysqlDB, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return nil, err
	}
	var mdb MysqlDB
	mdb.dbname = u.Path
	if strings.HasPrefix(mdb.dbname, "/") {
		mdb.dbname = mdb.dbname[1:]
	}
	if u.User.Username() == "" {
		return nil, fmt.Errorf("No user in connection url")
	}
	pass, _ := u.User.Password()
	if pass == "" {
		return nil, fmt.Errorf("No password in connection url")
	}
	if mdb.dbname == "" {
		return nil, fmt.Errorf("No database name in connection url")
	}
	dbconn := u.User.Username() + ":" +
		pass + "@tcp(" +
		u.Host + ")" +
		u.Path
	log.Debugln("momyre mysql conn", dbconn)
	mdb.client, err = sql.Open("mysql", dbconn)
	if err != nil {
		return nil, err
	}
	res, err := mdb.client.Query("SELECT name, value FROM momyre")
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if !ok {
			return nil, err
		}
		if me.Number == 1146 {
			log.Warnln("momyre mysql create a momyre table")
			err := createMomyreTable(&mdb)
			if err != nil {
				return nil, err
			}
			// repeat
			res, err = mdb.client.Query("SELECT name, value FROM momyre")
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	for res.Next() {
		var rec ConfRec
		err = res.Scan(&rec.Name, &rec.Value)
		if err != nil {
			return nil, err
		}
		log.Debugln("momyre mysql conf read:", rec.Name, string(rec.Value))
		if rec.Name == "timestamp" {
			s_ts := string(rec.Value)
			ts, err := strconv.ParseUint(s_ts, 10, 64)
			if err != nil {
				return nil, err
			}
			mdb.Timestamp = ts
		}
	}

	return &mdb, nil
}

func createMomyreTable(mdb *MysqlDB) error {
	sql := `

CREATE TABLE momyre (
  name varchar(100) NOT NULL,
  value blob,
  UNIQUE KEY momyre_name_IDX (name)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin

	`
	_, err := mdb.client.Exec(sql)
	log.Debugln("sql", sql)
	if err != nil {
		return err
	}
	return nil
}

func (mdb *MysqlDB) SyncTablesConfig() (map[string]map[string]bool, error) {
	// returns map of columns to resync from mongo
	resync_columns := make(map[string]map[string]bool)
	// remove all other tables
	res, err := mdb.client.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	var table string
	tables_remove := make([]string, 0)
	tables_exists := make(map[string]bool)
	for res.Next() {
		res.Scan(&table)
		if table == "momyre" {
			continue
		}
		if _, has := Conf.Tables_my[table]; !has {
			tables_remove = append(tables_remove, table)
		} else {
			tables_exists[table] = true
		}
	}
	if !*f_force && len(tables_remove) > 0 {
		return nil, fmt.Errorf("Cannot remove tables %v without --force", tables_remove)
	}
	for _, table := range tables_remove {
		log.Warnln("momyre mysql drop table", table)
		_, err := mdb.client.Exec("DROP TABLE " + table)
		if err != nil {
			return nil, err
		}
	}
	// append missing tables
	for table, _ := range Conf.Tables_my {
		if _, has := tables_exists[table]; has {
			continue
		}
		sql := `

CREATE TABLE $name (
  _id varchar(24) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY $name__id_IDX (_id)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin

		`
		sql = strings.ReplaceAll(sql, "$name", table)
		log.Warnln("momyre mysql create table", table)
		log.Debugln("sql", sql)
		_, err := mdb.client.Exec(sql)
		if err != nil {
			return nil, err
		}
	}

	for table, _ := range Conf.Tables_my {
		// what columns to resync
		resync_columns[table] = make(map[string]bool)
		// remove all other columns
		res, err := mdb.client.Query("SHOW COLUMNS FROM " + table)
		if err != nil {
			return nil, err
		}
		var column string
		var coltype string
		skip := make([]interface{}, 4)
		columns_remove := make([]string, 0)
		columns_exists := make(map[string]bool)
		for res.Next() {
			err := res.Scan(&column, &coltype, &skip[0], &skip[1], &skip[2], &skip[3])
			if err != nil {
				return nil, err
			}
			if column == "_id" {
				continue
			}
			if _, has := Conf.Tables_my[table][column]; !has {
				columns_remove = append(columns_remove, column)
			} else {
				def_coltype := Conf.Tables_my[table][column]
				if def_coltype == coltype {
					// exact match
					columns_exists[column] = true
				} else {
					// recreate the column
					log.Warnln("momyre mysql recreate column, types mismatch",
						table+"."+column,
						"has", coltype,
						"need", def_coltype)
					columns_remove = append(columns_remove, column)
				}
			}
		}
		if !*f_force && len(columns_remove) > 0 {
			return nil, fmt.Errorf("Cannot remove tables %s.%v without --force", table, columns_remove)
		}
		for _, col := range columns_remove {
			log.Warnln("momyre mysql drop column", table+"."+col)
			sql := "ALTER TABLE " + table + " DROP COLUMN `" + col + "`"
			log.Debugln("sql", sql)
			_, err := mdb.client.Exec(sql)
			if err != nil {
				return nil, err
			}
		}
		// append missing columns
		for col, _ := range Conf.Tables_my[table] {
			if _, has := columns_exists[col]; has {
				continue
			}
			sql := "ALTER TABLE `$table` ADD `$column` $coltype NULL"
			sql = strings.ReplaceAll(sql, "$table", table)
			sql = strings.ReplaceAll(sql, "$column", col)
			sql = strings.ReplaceAll(sql, "$coltype", Conf.Tables_my[table][col])
			log.Debugln("sql", sql)
			log.Warnln("momyre mysql create column", table+"."+col)
			_, err := mdb.client.Exec(sql)
			if err != nil {
				return nil, err
			}
			resync_columns[table][col] = true
		}
		// preserve order of columns
		sql := "ALTER TABLE `" + table + "` CHANGE _id _id varchar(24) FIRST"
		log.Debugln("sql", sql)
		_, err = mdb.client.Exec(sql)
		if err != nil {
			return nil, err
		}
		lastcol := "_id"
		for _, col := range Conf.Cols_my[table] {
			log.Debugln("momyre mysql order column", table+"."+col)
			coltype = Conf.Tables_my[table][col]
			sql := "ALTER TABLE `" + table + "` CHANGE `" + col + "` `" + col + "` " + coltype + " AFTER `" + lastcol + "`"
			log.Debugln("sql", sql)
			_, err := mdb.client.Exec(sql)
			if err != nil {
				return nil, err
			}
			lastcol = col
		}
		// preserve right order of columns
	}
	return resync_columns, nil
}

func (mdb *MysqlDB) upsertRow(
	table string,
	obj map[string]interface{}) error {

	err := mdb.appendRow(table, obj)
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if !ok {
			return err
		}
		if me.Number != 1062 { // not duplicate entry
			return err
		}
		return mdb.updateRow(table, obj)
	}
	return nil
}

func (mdb *MysqlDB) appendRow(
	table string,
	obj map[string]interface{}) error {

	cols := make([]string, 0, len(Conf.Tables_my[table])+1)
	cols = append(cols, "_id")
	for name, _ := range Conf.Tables_my[table] {
		cols = append(cols, name)
	}

	q_ns := ""
	q_vs := ""
	vals := make([]interface{}, 0, len(obj))
	for _, name := range cols {
		if len(q_ns) > 0 {
			q_ns += ","
			q_vs += ","
		}
		q_ns += "`" + name + "`"
		q_vs += "?"
		//now value
		vali := obj[name]
		switch val := vali.(type) {
		case primitive.ObjectID:
			vali = val.Hex()
		case primitive.A:
			bytes, _ := json.Marshal(val)
			vali = string(bytes)
		}
		vals = append(vals, vali)
	}
	sql := "INSERT INTO " + table + " (" + q_ns + ") VALUES (" + q_vs + ")"
	log.Debugln("sql", sql)
	q, err := mdb.client.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = q.Exec(vals...)
	if err != nil {
		return err
	}
	if ts, has := obj["$ts"]; has {
		tsnum, is_uint64 := ts.(uint64)
		if is_uint64 {
			err = mdb.updateTimestamp(tsnum)
			if err != nil {
				return err
			}
		} else {
			log.Infoln("momyre mysql $ts has wrong type")
		}
	}
	return nil
}

func (mdb *MysqlDB) updateRow(
	table string,
	obj map[string]interface{}) error {

	if _, has := Conf.Tables_my[table]; !has {
		return nil // skip
	}
	if _, has := obj["_id"]; !has {
		return fmt.Errorf("No _id in mongo object")
	}
	id_obj, is_oid := obj["_id"].(primitive.ObjectID)
	if !is_oid {
		return fmt.Errorf("_id is not ObjectID")
	}
	idhex := id_obj.Hex()
	cols := make([]string, 0, len(Conf.Tables_my[table]))
	for name, _ := range Conf.Tables_my[table] {
		cols = append(cols, name)
	}

	q_ns := ""
	vals := make([]interface{}, 0, len(cols)+1)
	for _, name := range cols {
		if _, has := obj[name]; !has {
			continue // no field
		}
		vali := obj[name]
		switch val := vali.(type) {
		case primitive.ObjectID:
			vali = val.Hex()
		case primitive.A:
			bytes, _ := json.Marshal(val)
			vali = string(bytes)
		}
		if len(q_ns) > 0 {
			q_ns += ","
		}
		q_ns += "`" + name + "`=?"
		vals = append(vals, vali)
	}
	if len(vals) == 0 {
		return nil // skip
	}
	vals = append(vals, idhex)

	sql := "UPDATE " + table + " SET " + q_ns + " WHERE _id=?"
	log.Debugln("sql", sql)
	q, err := mdb.client.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = q.Exec(vals...)
	if err != nil {
		return err
	}
	if ts, has := obj["$ts"]; has {
		tsnum, is_uint64 := ts.(uint64)
		if is_uint64 {
			err = mdb.updateTimestamp(tsnum)
			if err != nil {
				return err
			}
		} else {
			log.Infoln("momyre mysql $ts has wrong type")
		}
	}
	return nil
}

func (mdb *MysqlDB) deleteRow(
	table, idhex string, tsnum uint64) error {

	if _, has := Conf.Tables_my[table]; !has {
		return nil // skip
	}

	sql := "DELETE FROM " + table + " WHERE _id=?"
	log.Debugln("sql", sql)
	q, err := mdb.client.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = q.Exec(idhex)
	if err != nil {
		return err
	}
	if tsnum > 0 {
		err = mdb.updateTimestamp(tsnum)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mdb *MysqlDB) updateTimestamp(tsnum uint64) error {

	sql := "INSERT INTO momyre (name,value) VALUES (?,?)"
	q, err := mdb.client.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = q.Exec("timestamp", strconv.FormatUint(tsnum, 10))
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if !ok {
			return err
		}
		if me.Number != 1062 { // not duplicate entry
			return err
		}

		sql := "UPDATE momyre SET value=? WHERE name=?"
		q, err := mdb.client.Prepare(sql)
		if err != nil {
			return err
		}
		_, err = q.Exec(strconv.FormatUint(tsnum, 10), "timestamp")
		return err
	}
	mdb.Timestamp = tsnum
	return nil
}
