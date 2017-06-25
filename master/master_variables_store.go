package master

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/golang/protobuf/proto"
)

type MasterVariablesStore struct {
  LogStorage logstorage.LogStorage
}

func NewMasterVariablesStore(storage logstorage.LogStorage) *MasterVariablesStore {
  return &MasterVariablesStore{
    LogStorage:storage,
  }
}

func (self *MasterVariablesStore) Write(options logstorage.WriteOptions, groupIdx int32,
                                        variables common.MasterVariables) error {
  buffer, err := proto.Marshal(variables)
  if err != nil {
    log.Error("proto.marshal fail %v", err)
    return err
  }

  err = self.LogStorage.SetMasterVariables(options, groupIdx, buffer)
  if err != nil {
    log.Error("db.put fail %v, groupidx %d", err, groupIdx)
    return err
  }

  return nil
}

func (self *MasterVariablesStore) Read(groupIdx int32, variables *common.MasterVariables) error {
  var buffer []byte
  err := self.LogStorage.GetMasterVariables(groupIdx, buffer)
  if err != nil && err != common.ErrKeyNotFound {
    log.Error("db.get fail %v, group idx %d", err, groupIdx)
    return err
  }

  if err == common.ErrKeyNotFound {
    log.Error("db.get not found, group idx %d", groupIdx)
    return nil
  }

  err = proto.Unmarshal(buffer, variables)
  if err != nil {
    log.Error("proto.unmarshal fail %v", err)
    return err
  }

  return nil
}
