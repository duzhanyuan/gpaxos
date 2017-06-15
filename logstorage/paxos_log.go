package logstorage

import (
  "fmt"

  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
)

type PaxosLog struct {
  LogStorage LogStorage
}

func (self *PaxosLog) WriteLog(options WriteOptions, groupIdx int32, instanceId uint64, value string) error {
  var nullid uint64 = 0
  state := common.AcceptorStateData{
    InstanceID:     &instanceId,
    AcceptedValue:  []byte(value),
    PromiseID:      &nullid,
    PromiseNodeID:  &nullid,
    AcceptedID:     &nullid,
    AcceptedNodeID: &nullid,
  }

  err := self.WriteState(options, groupIdx, instanceId, state)
  if err != nil {
    log.Error("WriteState to db fail, groupidx %d instanceid %d err:%v", groupIdx, instanceId, err)
    return err
  }

  return nil
}

func (self *PaxosLog) ReadLog(groupIdx int32, instanceId uint64, value *string) error {
  var state common.AcceptorStateData
  err := self.ReadState(groupIdx, instanceId, &state)
  if err != nil {
    return err
  }

  *value = string(state.GetAcceptedValue())
  return nil
}

func (self *PaxosLog) WriteState(options WriteOptions, groupIdx int32, instanceId uint64, state common.AcceptorStateData) error {
  buf, err := proto.Marshal(&state)
  if err != nil {
    log.Error("State serialize error:%v", err)
    return err
  }

  err = self.LogStorage.Put(options, groupIdx, instanceId, buf)
  if err != nil {
    log.Error("DB.Put fail, groupidx %d bufferlen %d error %v", groupIdx, len(buf), err)
    return err
  }

  return nil
}

func (self *PaxosLog) ReadState(groupIdx int32, instanceId uint64, state *common.AcceptorStateData) error {
  var buf []byte
  err := self.LogStorage.Get(groupIdx, instanceId, &buf)
  if err != nil {
    log.Error("DB.Get fail,groupidx %d error %v", groupIdx, err)
    return err
  }
  if len(buf) == 0 {
    err = fmt.Errorf("DB.Get not found, groupidx %d", groupIdx)
    return err
  }

  err = proto.Unmarshal(buf, state)
  if err != nil {
    log.Error("State Unseriasize fail: %v", err)
    return err
  }

  return nil
}

// common.ErrKeyNotFound or nil
func (self *PaxosLog) GetMaxInstanceIdFromLog(groupIdx int32, instanceId *uint64) error {
  err := self.LogStorage.GetMaxInstanceId(groupIdx, instanceId)
  if err != nil {
    log.Error("db.getmax fail, groupid:%d error:%v", groupIdx, err)
    return err
  }

  return nil
}

func NewPaxosLog(logStorage LogStorage) *PaxosLog {
  return &PaxosLog{
    LogStorage:logStorage,
  }
}
