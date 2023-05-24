package main

import (
	"context"
	"flag"

	"github.com/ethereum/go-ethereum/common/hexutil"

	logging "github.com/ngchain/zap-log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var log = logging.Logger("sync")

var doFunc = flag.String("f", "", "db func: addHeight, ")
var mongoURI = flag.String("mongo", "mongodb://localhost:27017", "the uri of mongodb")
var dbName = flag.String("db", "", "the name of db in mongodb")
var collName = flag.String("coll", "", "the name of db collection in mongodb")
// var fieldName = flag.String("field", "", "the name of field in mongodb")
var fromFlag = flag.Uint64("from", 0, "from")
var fromFlag = flag.Uint64("to", 0, "to")

func main() {
	flag.Parse()

	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(*mongoURI))
	defer func() {
		if err = mongoClient.Disconnect(context.Background()); err != nil {
			panic(err)
		}
	}()

	switch *doFunc {
	case "":
		panic("f is required")
	case "addHeight":
		db := mongoClient.Database(*dbName)
		coll := db.Collection((*collName))
		addHeightField(coll)
	}
}

func addHeightField(coll *mongo.Collection) {
	MIN_HEIGHT := uint64(16_800_000)
	MAX_HEIGHT := uint64(16_900_000)
	ctx := context.TODO()
	for height := MIN_HEIGHT; height <= MAX_HEIGHT; height += 1 {
		blockNumber := hexutil.EncodeUint64(height)
		// docs = coll.Find(ctx, bson.M{"blockNumber": blockNumber})
		_, err := coll.UpdateMany(ctx, bson.M{"blockNumber": blockNumber}, bson.M{"$set": bson.M{"height": height}})
		if err != nil {
			panic(err)
		}

		log.Warn(height)
	}
}
