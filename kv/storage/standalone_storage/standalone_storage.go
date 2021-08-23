package standalone_storage

import (
	"log"
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
	txn := s.engines.Kv.NewTransaction(false)
	return &StandaloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			log.Printf("write put: %v, detail: cf=%v, key=%v, val=%v", put, put.Cf, put.Key, put.Value)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}
	err := wb.WriteToDB(s.engines.Kv)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}
	return nil
}

// reader
type StandaloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (r *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandaloneStorageReader) Close() {
	r.txn.Discard()
}
