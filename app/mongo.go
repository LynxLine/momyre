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
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDB struct {
	dbname  string
	timeout time.Duration
	client  *mongo.Client
	opts    *options.ClientOptions
}

func ConnectMo(conn string, timeout time.Duration) (*MongoDB, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return nil, err
	}
	var mdb MongoDB
	mdb.dbname = u.Path
	if strings.HasPrefix(mdb.dbname, "/") {
		mdb.dbname = mdb.dbname[1:]
	}
	if mdb.dbname == "" {
		return nil, fmt.Errorf("No database name in connection url")
	}
	mdb.timeout = timeout
	mdb.opts = options.Client().ApplyURI(conn)
	u_mo, _ := url.Parse(Conf.Out)
	if u_mo.User != nil {
		user := u_mo.User.Username()
		pass, _ := u_mo.User.Password()
		cred := options.Credential{
			AuthMechanism: "SCRAM-SHA-256",
			Username:      user,
			Password:      pass,
		}
		mdb.opts.SetAuth(cred)
	}
	mdb.client, err = mongo.NewClient(mdb.opts)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), mdb.timeout)
	err = mdb.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	ctx, _ = context.WithTimeout(context.Background(), mdb.timeout)
	err = mdb.client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}
	return &mdb, nil
}

func (mdb *MongoDB) Run(

	ns []string,
	timestamp uint64,
	in_ch chan<- map[string]interface{},
	up_ch chan<- map[string]interface{},
	de_ch chan<- map[string]interface{}) error {

	for {
		err := mdb.readLogs(ns, timestamp, in_ch, up_ch, de_ch)
		if err != nil {
			return err
		}
	}
}

type OpLog struct {
	Ts primitive.Timestamp    `bson:"ts"`
	Ns string                 `bson:"ns"`
	Op string                 `bson:"op"`
	O1 map[string]interface{} `bson:"o"`
	O2 map[string]interface{} `bson:"o2"`
}

type TxO1 struct {
	Ops []OpLog `bson:"applyOps"`
}

type TxLog struct {
	Ts primitive.Timestamp    `bson:"ts"`
	Ns string                 `bson:"ns"`
	Op string                 `bson:"op"`
	O1 TxO1                   `bson:"o"`
	O2 map[string]interface{} `bson:"o2"`
}

func obj2add(x, y map[string]interface{}, pref string) {
	for k, o := range x {
		m, is_map := o.(map[string]interface{})
		if is_map {
			obj2add(m, y, k+"_")
			continue
		}
		y[pref+k] = o
	}
}
func obj2plain(x map[string]interface{}) map[string]interface{} {
	y := make(map[string]interface{})
	obj2add(x, y, "")
	return y
}

func (mdb *MongoDB) handleChange(
	ch *OpLog,
	cur *mongo.Cursor,
	ns_map map[string]bool,
	in_ch chan<- map[string]interface{},
	up_ch chan<- map[string]interface{},
	de_ch chan<- map[string]interface{}) (bool, error) {

	if ch.Op == "i" {
		ops := ch.O1
		table := ch.Ns
		if strings.HasPrefix(table, mdb.dbname+".") {
			n := len(mdb.dbname) + 1
			table = table[n:len(table)]
		}
		ops["$ns"] = table
		ops["$ts"] = uint64(ch.Ts.T)<<32 + uint64(ch.Ts.I)

		in_ch <- obj2plain(ops)

		return true, nil
	}

	if ch.Op == "u" {
		if _, has := ch.O2["_id"]; !has {
			log.Fatalln("momyre mongo log u: no _id in o2, skip")
			return true, nil // skip
		}
		id_obj, is_oid := ch.O2["_id"].(primitive.ObjectID)
		if !is_oid {
			log.Fatalln("momyre mongo log u: _id is not ObjectID, skip")
			return true, nil // skip
		}
		// $set
		if _, has := ch.O1["$set"]; has {
			ops, is_map := ch.O1["$set"].(map[string]interface{})
			if !is_map {
				log.Fatalln("momyre mongo log u: $set is not map")
				return true, nil // skip
			}
			ops["_id"] = id_obj
			table := ch.Ns
			if strings.HasPrefix(table, mdb.dbname+".") {
				n := len(mdb.dbname) + 1
				table = table[n:len(table)]
			}
			ops["$ns"] = table
			ops["$ts"] = uint64(ch.Ts.T)<<32 + uint64(ch.Ts.I)
			//log.Infoln("ops", ops)
			up_ch <- obj2plain(ops)
			return true, nil // done
		}
		if _, has := ch.O1["$v"]; has {
			ver := ch.O1["$v"].(int)
			if ver == 2 {
				diff, is_map := ch.O1["diff"].(map[string]interface{})
				if !is_map {
					log.Fatalln("momyre mongo log u: $v=2, diff is not map")
					return true, nil // skip
				}
				if _, has := diff["u"]; has {
					ops, is_map := diff["u"].(map[string]interface{})
					if !is_map {
						log.Fatalln("momyre mongo log u: $v=2, diff.u is not map")
						return true, nil // skip
					}
					ops["_id"] = id_obj
					table := ch.Ns
					if strings.HasPrefix(table, mdb.dbname+".") {
						n := len(mdb.dbname) + 1
						table = table[n:len(table)]
					}
					ops["$ns"] = table
					ops["$ts"] = uint64(ch.Ts.T)<<32 + uint64(ch.Ts.I)
					//log.Infoln("ops", ops)
					up_ch <- obj2plain(ops)
					return true, nil // done
				}
			}
		}

		// not supported u op
		data, _ := json.MarshalIndent(ch.O1, "", "  ")
		log.Fatalln("not supported u op", string(data))

		return true, nil // skip
	}

	if ch.Op == "d" {
		if _, has := ch.O1["_id"]; !has {
			log.Infoln("momyre mongo log u: no _id in o1, skip")
			return true, nil // skip
		}
		id_obj, is_oid := ch.O1["_id"].(primitive.ObjectID)
		if !is_oid {
			log.Infoln("momyre mongo log u: _id is not ObjectID, skip")
			return true, nil // skip
		}
		ops := make(map[string]interface{})
		ops["_id"] = id_obj.Hex()
		table := ch.Ns
		if strings.HasPrefix(table, mdb.dbname+".") {
			n := len(mdb.dbname) + 1
			table = table[n:len(table)]
		}
		ops["$ns"] = table
		ops["$ts"] = uint64(ch.Ts.T)<<32 + uint64(ch.Ts.I)
		de_ch <- ops
		return true, nil // done
	}

	if ch.Op == "n" {
		// skip no-op log item
		return true, nil // done
	}

	if ch.Op == "c" && ch.Ns == "admin.$cmd" {

		var tx TxLog
		err := cur.Decode(&tx)
		if err != nil {
			return false, err
		}

		for _, ch1 := range tx.O1.Ops {
			_, has := ns_map[ch1.Ns]
			if !has {
				continue
			}
			ch1.Ts = tx.Ts
			_, err := mdb.handleChange(
				&ch1, cur,
				ns_map,
				in_ch, up_ch, de_ch)
			if err != nil {
				log.Errorln("momyre mongo tx err", err)
			}
		}

		return true, nil // done
	}

	return false, nil // not processed
}

func (mdb *MongoDB) readLogs(

	tables []string,
	timestamp uint64,
	in_ch chan<- map[string]interface{},
	up_ch chan<- map[string]interface{},
	de_ch chan<- map[string]interface{}) error {

	q := bson.D{}
	ns_map := make(map[string]bool)

	ns := make([]string, 0, len(tables)+1)
	for _, table := range tables {
		ns = append(ns, mdb.dbname+"."+table)
		ns_map[mdb.dbname+"."+table] = true
	}
	ns = append(ns, "admin.$cmd")
	{ // ns
		el := bson.E{
			Key: "ns",
			Value: bson.M{
				"$in": ns,
			},
		}
		q = append(q, el)
	}

	{ // ts
		el := bson.E{
			Key: "ts",
			Value: bson.D{{
				"$gt", primitive.Timestamp{
					T: uint32(timestamp >> 32),
					I: uint32(timestamp % (1 << 32)),
				},
			}},
		}
		q = append(q, el)
	}

	fo := &options.FindOptions{}
	fo.SetCursorType(options.TailableAwait)
	fo.SetMaxTime(mdb.timeout)
	fo.SetBatchSize(100)

	cl := mdb.client.Database("local").Collection("oplog.rs")
	ctx := context.TODO()
	cur, err := cl.Find(ctx, q, fo)
	if err != nil {
		return err
	}

	defer cur.Close(ctx)
	for cur.Next(ctx) {

		var ch OpLog
		err := cur.Decode(&ch)
		if err != nil {
			return err
		}

		done, err := mdb.handleChange(&ch, cur, ns_map, in_ch, up_ch, de_ch)
		if done {
			continue
		}
		if err != nil {
			log.Errorln("momyre mongo ch handle:", err)
			continue
		}

		data, _ := json.MarshalIndent(ch, "", "  ")
		log.Println("current", string(data))
	}

	if err := cur.Err(); err != nil {
		return err
	}

	return nil // ?
}

func (mdb *MongoDB) Tx(timeout time.Duration, todo func(ctx context.Context) error) error {

	tx_ctx, _ := context.WithTimeout(context.Background(), timeout)
	s, err := mdb.client.StartSession()
	if err != nil {
		return err
	}
	defer s.EndSession(tx_ctx)
	err = s.StartTransaction()
	if err != nil {
		return err
	}
	if err = mongo.WithSession(tx_ctx, s, func(ctx mongo.SessionContext) error {
		if err = todo(ctx); err != nil {
			return fmt.Errorf("Tx: todo: %+v\n", err)
		}
		if err = s.CommitTransaction(ctx); err != nil {
			return fmt.Errorf("Tx: commit: %+v\n", err)
		}
		return nil

	}); err != nil {
		return err
	}

	return err
}

func (mdb *MongoDB) readTable(
	table string,
	out_ch chan<- map[string]interface{}) error {

	ctx := context.TODO()
	defer close(out_ch)
	q := bson.D{}

	cl := mdb.client.Database(mdb.dbname).Collection(table)
	cur, err := cl.Find(ctx, q)
	if err != nil {
		return err
	}

	defer cur.Close(ctx)
	for cur.Next(ctx) {
		obj := make(map[string]interface{})
		err := cur.Decode(&obj)
		if err != nil {
			return err
		}
		out_ch <- obj2plain(obj)
	}
	return nil
}

func (mdb *MongoDB) readTimestamp() (uint64, error) {

	ctx := context.TODO()
	cl := mdb.client.Database("local").Collection("oplog.rs")
	q := bson.D{}
	fo := options.FindOne()
	fo.SetSort(bson.D{{"$natural", -1}})

	obj := make(map[string]interface{})
	err := cl.FindOne(ctx, q, fo).Decode(&obj)
	if err != nil {
		return 0, err
	}
	ts, is_ts := obj["ts"].(primitive.Timestamp)
	if !is_ts {
		return 0, fmt.Errorf("ts is not Timestamp object")
	}

	tsnum := uint64(ts.T)<<32 + uint64(ts.I)

	return tsnum, nil
}

func (mdb *MongoDB) pauseWrites() error {
	cmd := bson.D{{"fsync", 1}, {"lock", true}}
	opts := options.RunCmd().SetReadPreference(readpref.Primary())
	res := mdb.client.Database("admin").RunCommand(context.TODO(), cmd, opts)
	if err := res.Err(); err != nil {
		return err
	}
	obj := make(map[string]interface{})
	err := res.Decode(&obj)
	if err != nil {
		return err
	}
	if _, has := obj["lockCount"]; !has {
		return fmt.Errorf("no lockCount in response")
	}
	lockCount, is_int := obj["lockCount"].(int64)
	if !is_int {
		return fmt.Errorf("lockCount is not int64")
	}
	info, _ := obj["info"].(string)
	log.Infoln("momyre mongo pause replication:", info, "lockCount:", lockCount)
	return nil
}

func (mdb *MongoDB) unpauseWrites() error {
	lastLockCount := int64(1000000)
	for {
		cmd := bson.D{{"fsyncUnlock", 1}}
		opts := options.RunCmd().SetReadPreference(readpref.Primary())
		res := mdb.client.Database("admin").RunCommand(context.TODO(), cmd, opts)
		if err := res.Err(); err != nil {
			return err
		}
		obj := make(map[string]interface{})
		err := res.Decode(&obj)
		if err != nil {
			return err
		}
		if _, has := obj["lockCount"]; !has {
			return fmt.Errorf("no lockCount in response")
		}
		lockCount, is_int := obj["lockCount"].(int64)
		if !is_int {
			return fmt.Errorf("lockCount is not int64")
		}
		if lastLockCount == lockCount {
			return fmt.Errorf("lockCount not changed")
		}
		info, _ := obj["info"].(string)
		log.Infoln("momyre mongo unpause replication:", info, "lockCount:", lockCount)
		if lockCount == 0 {
			break
		}
		lastLockCount = lockCount

	}
	return nil
}
