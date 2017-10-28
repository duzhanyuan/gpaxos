package logstorage

import (
  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos/common"
  log "github.com/lichuang/log4go"
)

type SystemVariablesStore struct {
  LogStorage *LogStorage
}

func NewSystemVariablesStore(storage *LogStorage) *SystemVariablesStore {
  return &SystemVariablesStore{
    LogStorage:storage,
  }
}

func (self *SystemVariablesStore) Write(options WriteOptions, groupIdx int32, variables *common.SystemVariables) error {
  buf, err := proto.Marshal(variables)
  if err != nil {
    log.Error("Variables.Serialize fail:%v", err)
    return err
  }

  err = self.LogStorage.SetSystemVariables(options, buf)
  if err != nil {
    log.Error("db.put fail, groupidx %d bufferlen %d err:%v", groupIdx, len(buf), err)
    return err

  }

  return nil
}

func (self *SystemVariablesStore) Read(groupIdx int32, variables *common.SystemVariables) error {
  buffer, err := self.LogStorage.GetSystemVariables()
  if err != nil {
    log.Error("db.get fail, groupidx %d err:%v", groupIdx, err)
    return err
  }

  err = proto.Unmarshal(buffer, variables)
  if err != nil {
    log.Error("Variables.ParseFromArray fail, bufferlen %d", len(buffer))
    return err
  }
  return nil
}

