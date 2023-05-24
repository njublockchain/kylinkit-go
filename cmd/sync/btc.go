package main

import (
	"context"
	"io"
	"os"

	"github.com/btcsuite/btcd/rpcclient"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BtcSyncMan struct {
	chainName    string
	dbName       string
	mongoClient  *mongo.Client
	btcRpcClient *rpcclient.Client
}

func NewBtcSyncMan(chainName, dbName string, mongoClient *mongo.Client) *BtcSyncMan {
	certFile, err := os.Open("rpc.cert")
	chk(err)
	cert, err := io.ReadAll(certFile)
	chk(err)
	btcdConfig := &rpcclient.ConnConfig{
		Host:         "127.0.0.1:8334",
		User:         "btcd",
		Pass:         "pass",
		Certificates: cert,
		HTTPPostMode: true,
		// DisableTLS:   true,
	}
	btcRpcClient, err := rpcclient.New(btcdConfig, nil)
	chk(err)

	return &BtcSyncMan{
		chainName:    chainName,
		dbName:       dbName,
		mongoClient:  mongoClient,
		btcRpcClient: btcRpcClient,
	}
}

func (s *BtcSyncMan) Preload(end uint64) {
	db := s.mongoClient.Database(s.dbName)
	blockColl := db.Collection("blockColl")
	transactionColl := db.Collection("transactionColl")

	ctx := context.Background()
	localLatest := getLocalLatestBtcBlock(ctx, blockColl)
	log.Warn(localLatest)

	_, remoteLatest, err := s.btcRpcClient.GetBestBlock()
	chk(err)
	log.Warn(remoteLatest)

	safeLatest := int64(remoteLatest) / 1_00 * 1_00

	for start := int64(localLatest + 1); start <= safeLatest; start++ {
		hash, err := s.btcRpcClient.GetBlockHash(start)
		chk(err)
		block, err := s.btcRpcClient.GetBlockVerboseTx(hash)
		chk(err)

		if len(block.Tx) == 0 {
			block.Tx = block.RawTx
			block.RawTx = nil
		}

		txs := block.Tx
		if len(txs) > 0 {
			transactionModels := make([]mongo.WriteModel, len(txs))

			for i, tx := range txs {
				transactionModels[i] = mongo.NewInsertOneModel().SetDocument(tx)
			}

			_, err = transactionColl.BulkWrite(ctx, transactionModels)
			chk(err)
		}

		_, err = blockColl.InsertOne(ctx, block)
		chk(err)

		log.Warnf("inserted block @ %d (%d txs)", start, len(txs))
	}
}

func getLocalLatestBtcBlock(ctx context.Context, blockColl *mongo.Collection) int64 {
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

	height, _ := results[0]["height"].(int64)
	return height

}

func (s *BtcSyncMan) Sync()          {}
func (s *BtcSyncMan) MaintainState() {}

func (s *BtcSyncMan) Check() {
	db := s.mongoClient.Database(s.dbName)
	blockColl := db.Collection("blockColl")
	transactionColl := db.Collection("transactionColl")
	// receiptColl := db.Collection("receiptColl")

	ctx := context.Background()
	localLatest := getLocalLatestBtcBlock(ctx, blockColl)
	log.Warn(localLatest)

	// _, remoteLatest, err := s.btcRpcClient.GetBestBlock()
	// chk(err)
	// log.Warn(remoteLatest)

	// safeLatest := int64(remoteLatest) / 1_00 * 1_00

	var inserted = 0
	for start := int64(0); start <= localLatest; start++ {
		hash, err := s.btcRpcClient.GetBlockHash(start)
		chk(err)
		block, err := s.btcRpcClient.GetBlockVerboseTx(hash)
		chk(err)

		if len(block.Tx) == 0 {
			block.Tx = block.RawTx
			block.RawTx = nil
		}

		inserted = 0
		for _, tx := range block.Tx {
			err := transactionColl.FindOne(ctx, bson.M{"hash": tx.Hash}).Err()
			if err != nil {
				if err != mongo.ErrNoDocuments {
					panic(err)
				}

				_, err = transactionColl.InsertOne(ctx, tx)
				chk(err)
				inserted++
			}
		}

		_, err = blockColl.InsertOne(ctx, block)
		chk(err)
		log.Warnf("checked block @ %d (%d/%d txs) ", start, inserted, len(block.Tx))
	}
}
