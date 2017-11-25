package master

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/storage"
  "github.com/golang/protobuf/proto"
  log "github.com/lichuang/log4go"
)

type MasterVariablesStore struct {
  LogStorage *storage.LogStorage
}

func NewMasterVariablesStore(storage *storage.LogStorage) *MasterVariablesStore {
  return &MasterVariablesStore{
    LogStorage:storage,
  }
}

func (self *MasterVariablesStore) Write(options storage.WriteOptions, groupIdx int32,
  variables common.MasterVariables) error {
  buffer, err := proto.Marshal(&variables)
  if err != nil {
    log.Error("proto.marshal fail %v", err)
    return err
  }

  err = self.LogStorage.SetMasterVariables(options, buffer)
  if err != nil {
    log.Error("db.put fail %v, groupidx %d", err, groupIdx)
    return err
  }

  return nil
}

func (self *MasterVariablesStore) Read(groupIdx int32, variables *common.MasterVariables) error {
  buffer, err := self.LogStorage.GetMasterVariables()
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
