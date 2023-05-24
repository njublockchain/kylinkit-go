package main

import (
	"context"
	"flag"
	"reflect"
	"strings"

	logging "github.com/ngchain/zap-log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var log = logging.Logger("sync")

var isCheck = flag.Bool("check", false, "do data integrity check")
var isPreload = flag.Bool("preload", false, "do data preload")
var isStateUpdate = flag.Bool("state", false, "do state update for all accounts")

var mongoURI = flag.String("mongo", "mongodb://localhost:27017", "the uri of mongodb")
var chainName = flag.String("chain", "", "name of the chain (btc/eth)")
var dbName = flag.String("db", "", "the name of db in mongodb (chainname+'rpc' by default)")
var end = flag.Uint64("end", 0, "the end for preload")

var ethURI = flag.String("eth", "http://localhost:8545", "the uri of eth daemon, IPC is suggested (most stable)")

// TODO
// var btcURI = flag.String("eth", "http://localhost:8545", "the uri of eth daemon, IPC is suggested (most stable)")

type SyncMan interface {
	Preload(uint64)
	Sync()
	MaintainState()
	Check()
}

func chk(err error) {
	if err != nil {
		panic(err)
	}

}

func main() {
	flag.Parse()

	if *chainName == "" {
		panic("chainName is required")
	}

	if *dbName == "" {
		*dbName = *chainName + "rpc"
	}

	// json tag
	structcodec, _ := bsoncodec.NewStructCodec(bsoncodec.JSONFallbackStructTagParser)
	rb := bson.NewRegistryBuilder()
	rb.RegisterDefaultEncoder(reflect.Struct, structcodec)
	rb.RegisterDefaultDecoder(reflect.Struct, structcodec)
	clientOptions := options.Client().SetRegistry(rb.Build()).ApplyURI(*mongoURI)

	mongoClient, err := mongo.Connect(context.Background(), clientOptions)
	defer func() {
		if err = mongoClient.Disconnect(context.Background()); err != nil {
			panic(err)
		}
	}()
	err = mongoClient.Ping(context.Background(), readpref.Primary())

	var man SyncMan
	if strings.Contains(*chainName, "eth") {
		man = NewEthSyncMan(*chainName, *dbName, mongoClient)
	} else if strings.Contains(*chainName, "btc") {
		man = NewBtcSyncMan(*chainName, *dbName, mongoClient)
	} else if strings.Contains(*chainName, "tron") {
		man = NewTronSyncMan(*chainName, *dbName, mongoClient)
	}

	if *isCheck {
		man.Check()
	}
	if *isPreload {
		man.Preload(*end)
	}
	if *isStateUpdate {
		man.MaintainState()
	}

	// start daily sync
	// omit it
	// localHeight :=
}
