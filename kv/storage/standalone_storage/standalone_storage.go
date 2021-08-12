package standalone_storage

import (
	"os"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)

	kv := engine_util.CreateDB(kvPath, false)
	raft := engine_util.CreateDB(raftPath, true)
	engines := engine_util.NewEngines(kv, raft, kvPath, raftPath)

	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engines.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engines.Kv.NewTransaction(true)
	return &StandaloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}
	return wb.WriteToDB(s.engines.Kv)
}

// reader
type StandaloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandaloneStorageReader) Close() {
	r.txn.Discard()
}
