package main

import (
	"context"
	"encoding/json"
	"math/big"

	"git.ngx.fi/c0mm4nd/tronetl/tron"
	"github.com/ethereum/go-ethereum/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TronSyncMan struct {
	chainName     string
	dbName        string
	mongoClient   *mongo.Client
	tronRpcClient *tron.TronClient
}

func NewTronSyncMan(chainName, dbName string, mongoClient *mongo.Client) *TronSyncMan {
	tronRpcClient := tron.NewTronClient("http://localhost")

	return &TronSyncMan{
		chainName:     chainName,
		dbName:        dbName,
		mongoClient:   mongoClient,
		tronRpcClient: tronRpcClient,
	}
}

func (s *TronSyncMan) Preload(end uint64) {
	db := s.mongoClient.Database(s.dbName)
	blockColl := db.Collection("blockColl")
	transactionColl := db.Collection("transactionColl")
	txInfoColl := db.Collection("transactionInfotColl")

	ctx := context.Background()
	localLatest := getLocalLatestTronBlock(ctx, blockColl)
	log.Warn(localLatest)

	block := s.tronRpcClient.GetJSONBlockByNumber(nil, true)
	remoteLatest := block.Number
	log.Warn(remoteLatest)
	safeLatest := uint64(*remoteLatest) / 1_000 * 1_000

	for start := uint64(localLatest + 1); start < safeLatest; start++ {
		height := new(big.Int).SetUint64(start)
		block := s.tronRpcClient.GetHTTPBlockByNumber(height)

		txs := block.Transactions

		if len(txs) > 0 {
			transactionModels := make([]mongo.WriteModel, len(txs))

			for i, tx := range txs {
				j, err := json.Marshal(tx)
				chk(err)
				var docTx any
				err = json.Unmarshal(j, &docTx)
				chk(err)
				transactionModels[i] = mongo.NewInsertOneModel().SetDocument(docTx)
			}
			_, err := transactionColl.BulkWrite(ctx, transactionModels)
			chk(err)

			if start > 0 {
				txInfos := s.tronRpcClient.GetTxInfosByNumber(start)
				if len(txInfos) > 0 {
					txInfoModels := make([]mongo.WriteModel, len(txInfos))
					for i, txInfo := range txInfos {
						txInfoModels[i] = mongo.NewInsertOneModel().SetDocument(txInfo)
					}

					_, err = txInfoColl.BulkWrite(ctx, txInfoModels)
					chk(err)
				}

			}
		}

		block.Transactions = nil
		_, err := blockColl.InsertOne(ctx, block)
		chk(err)

		log.Warnf("inserted block @ %d (%d txs)", start, len(txs))
	}
}

func (s *TronSyncMan) Sync() {
	// TODO
}

func (s *TronSyncMan) Check() {

}

func (s *TronSyncMan) updateAccounts(ctx context.Context, addrs []common.Address) error {
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

func (s *TronSyncMan) MaintainState() {
}

func getLocalLatestTronBlock(ctx context.Context, blockColl *mongo.Collection) int64 {
	count, err := blockColl.EstimatedDocumentCount(ctx)
	if err != nil {
		panic(err)
	}

	if count == 0 {
		return -1
	}

	opts := options.Find()
	opts.SetSort(bson.D{{"block_header.raw_data.number", -1}})
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
	switch height := results[0]["block_header"].(map[string]any)["raw_data"].(map[string]any)["number"].(type) {
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
