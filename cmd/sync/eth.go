package main

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EthSyncMan struct {
	chainName    string
	dbName       string
	mongoClient  *mongo.Client
	ethRpcClient *rpc.Client
}

func NewEthSyncMan(chainName, dbName string, mongoClient *mongo.Client) *EthSyncMan {
	var ethRpcClient *rpc.Client
	ethRpcClient, err := rpc.Dial(*ethURI)
	if err != nil {
		panic(err)
	}

	return &EthSyncMan{
		chainName:    chainName,
		dbName:       dbName,
		mongoClient:  mongoClient,
		ethRpcClient: ethRpcClient,
	}
}

func (s *EthSyncMan) Preload(end uint64) {
	db := s.mongoClient.Database(s.dbName)
	blockColl := db.Collection("blockColl")
	transactionColl := db.Collection("transactionColl")
	receiptColl := db.Collection("receiptColl")

	ctx := context.Background()
	localLatest := getLocalLatestEthBlock(ctx, blockColl)
	log.Warn(localLatest)

	var remoteBlock map[string]any
	err := s.ethRpcClient.CallContext(ctx, &remoteBlock, "eth_getBlockByNumber", toBlockNumArg(nil), false)
	chk(err)
	remoteLatest := hexutil.MustDecodeUint64(remoteBlock["number"].(string))
	log.Warn(remoteLatest)

	log.Warn(end)

	for start := uint64(localLatest + 1); start <= end; start++ {
		var block map[string]any
		err := s.ethRpcClient.CallContext(ctx, &block, "eth_getBlockByNumber", toBlockNumArg(new(big.Int).SetUint64(start)), true)
		chk(err)
		// add height
		height := hexutil.MustDecodeUint64(block["number"].(string))
		block["height"] = height
		block["time"] = hexutil.MustDecodeUint64(block["timestamp"].(string))
		// log.Warn(block)

		txs := block["transactions"].([]any)
		var receipts []map[string]any
		err = s.ethRpcClient.CallContext(ctx, &receipts, "eth_getBlockReceipts", toBlockNumArg(new(big.Int).SetUint64(start)))
		chk(err)

		if len(txs) != len(receipts) {
			log.Error(len(txs), len(receipts))
			panic("invalid receipts len")
		}

		if len(txs) > 0 {
			transactionModels := make([]mongo.WriteModel, len(txs))
			receiptModels := make([]mongo.WriteModel, len(txs))

			for i, tx := range txs {
				tx := tx.(map[string]any)
				tx["height"] = height
				transactionModels[i] = mongo.NewInsertOneModel().SetDocument(tx)
			}

			for i, receipt := range receipts {
				receipt["height"] = height
				receiptModels[i] = mongo.NewInsertOneModel().SetDocument(receipt)
			}

			_, err = transactionColl.BulkWrite(ctx, transactionModels)
			chk(err)
			_, err = receiptColl.BulkWrite(ctx, receiptModels)
			chk(err)
		}

		delete(block, "transactions")
		_, err = blockColl.InsertOne(ctx, block)
		chk(err)

		log.Warnf("inserted block @ %d (%d txs)", start, len(txs))
	}
}

func (s *EthSyncMan) Sync() {
	// TODO
}

func (s *EthSyncMan) Check() {

}

func (s *EthSyncMan) updateAccounts(ctx context.Context, addrs []common.Address) error {
	db := s.mongoClient.Database(s.dbName)
	state := db.Collection("stateColl")
	// opts := options.CreateIndexes()

	state.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{"account": 1},
	})

	// TODO: for loop get accounts
	opts := options.Update()
	opts.SetUpsert(true)
	_, err := state.UpdateMany(ctx, []any{}, opts)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (s *EthSyncMan) MaintainState() {
	db := s.mongoClient.Database(s.dbName)
	stateColl := db.Collection("stateColl")

	// opts := options.CreateIndexes()
	ctx := context.Background()
	_, _ = stateColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{"address": 1},
	}) // ignore index build err

	log.Warn("start scan blk.miner")
	blockColl := db.Collection("blockColl")
	transactionColl := db.Collection("transactionColl")
	opts := options.Aggregate()
	opts.SetAllowDiskUse(true)
	opts.SetBatchSize(1) // avoid cur time out
	// opts.SetMaxAwaitTime(time.Hour)
	cur, err := blockColl.Aggregate(ctx, []any{
		bson.M{"$group": bson.M{"_id": "$miner"}}, // ensure index before using
		bson.M{"$project": bson.M{"_id": 1}},
	}, opts)
	if err != nil {
		panic(err)
	}

	// accounts := make([]any)
	ctx = context.Background()
	for {
		if cur.TryNext(ctx) {
			var result bson.D
			if err := cur.Decode(&result); err != nil {
				log.Fatal(err)
			}
			// fmt.Println(result)
			addr := result.Map()["_id"].(string)
			s.initiallyUpsertAccount(ctx, stateColl, addr)
			continue
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		if cur.ID() == 0 {
			break
		}
	}

	log.Warn("start scan tx.from")
	cur, err = transactionColl.Aggregate(ctx, []any{
		bson.M{"$group": bson.M{"_id": "$from"}}, // ensure index before using
		bson.M{"$project": bson.M{"_id": 1}},
	}, opts)
	if err != nil {
		panic(err)
	}

	ctx = context.Background()
	for {
		if cur.TryNext(ctx) {
			var result bson.D
			if err := cur.Decode(&result); err != nil {
				log.Fatal(err)
			}
			// fmt.Println(result)
			addr := result.Map()["_id"].(string)
			s.initiallyUpsertAccount(ctx, stateColl, addr)
			continue
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		if cur.ID() == 0 {
			break
		}
	}

	log.Warn("start scan tx.to")
	cur, err = transactionColl.Aggregate(ctx, []any{
		bson.M{"$group": bson.M{"_id": "$to"}}, // ensure index before using
		bson.M{"$project": bson.M{"_id": 1}},
	}, opts)
	if err != nil {
		panic(err)
	}

	for {
		if cur.TryNext(context.TODO()) {
			var result bson.D
			if err := cur.Decode(&result); err != nil {
				log.Fatal(err)
			}
			// fmt.Println(result)
			addr := result.Map()["_id"].(string)
			s.initiallyUpsertAccount(ctx, stateColl, addr)
			continue
		}
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
		if cur.ID() == 0 {
			break
		}
	}

	log.Warn("account done")
}

func (s EthSyncMan) initiallyUpsertAccount(ctx context.Context, stateColl *mongo.Collection, addr string) {
	var hexBal, hexCode, hexStorage string

	elems := []rpc.BatchElem{
		{
			Method: "eth_getBalance",
			Args: []any{
				addr,
				"latest",
			},
			Result: &hexBal,
		},
		{
			Method: "eth_getCode",
			Args: []any{
				addr,
				"latest",
			},
			Result: &hexCode,
		},
		{
			Method: "eth_getStorageAt", //https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getstorageat
			Args: []any{
				addr,
				"0x0",
				"latest",
			},
			Result: &hexStorage,
		},
	}
	err := s.ethRpcClient.BatchCallContext(ctx, elems)
	if err != nil {
		panic(err)
	}
	for _, elem := range elems {
		if elem.Error != nil {
			panic(elem.Error)
		}
	}

	bigBal := hexutil.MustDecodeBig(hexBal)
	balanceInEther, _ := weiToEther(bigBal).Float64()
	acc := map[string]any{
		"address":    addr,
		"hexBalance": hexBal,
		"balance":    balanceInEther,
		"code":       hexCode,
		"storage":    hexStorage,
	}

	opts := options.Update()
	opts.SetUpsert(true)
	_, err = stateColl.UpdateOne(ctx, bson.M{"address": addr}, bson.M{"$set": acc}, opts)
	if err != nil {
		panic(err)
	}

	log.Warnf("updated account %s", addr)
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	pending := big.NewInt(-1)
	if number.Cmp(pending) == 0 {
		return "pending"
	}
	return hexutil.EncodeBig(number)
}

func getLocalLatestEthBlock(ctx context.Context, blockColl *mongo.Collection) int64 {
	count, err := blockColl.EstimatedDocumentCount(ctx)
	if err != nil {
		panic(err)
	}

	if count == 0 {
		return -1
	}

	opts := options.Find()
	opts.SetSort(bson.D{{"height", -1}})
	opts.SetLimit(1)
	cur, err := blockColl.Find(ctx, bson.D{{}}, opts)
	if err != nil {
		panic(err)
	}
	var results []map[string]any
	err = cur.All(ctx, &results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		return -1
	}
	switch height := results[0]["height"].(type) {
	case int64:
		return height
	case int32:
		return int64(height)
	case uint32:
		return int64(height)
	case uint64:
		return int64(height)
	default:
		panic("unknown height")
	}

}

func weiToEther(wei *big.Int) *big.Float {
	return new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(params.Ether))
}
