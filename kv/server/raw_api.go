package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	} else {
		defer reader.Close()
		v, err := reader.GetCF(req.Cf, req.Key)
		if err != nil {
			return nil, err
		}
		return &kvrpcpb.RawGetResponse{RegionError: nil, Value: v, NotFound: v == nil}, nil
	}
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{Data: storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}},
	})
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{Data: storage.Delete{Cf: req.Cf, Key: req.Key}},
	})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	} else {
		defer reader.Close()

		var res []*kvrpcpb.KvPair
		it := reader.IterCF(req.GetCf())
		it.Seek(req.StartKey)
		for i := 0; it.Valid() && i < int(req.GetLimit()); i++ {
			item := it.Item()
			pair := new(kvrpcpb.KvPair)
			pair.Key = item.KeyCopy(nil)
			pair.Value, err = item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			res = append(res, pair)
			it.Next()
		}
		it.Close()
		return &kvrpcpb.RawScanResponse{RegionError: nil, Kvs: res}, nil
	}
}
