package storage

import (
  "fmt"

  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos/common"

  log "github.com/lichuang/log4go"
)

type PaxosLog struct {
  logStorage *LogStorage
}

func (self *PaxosLog) WriteLog(options WriteOptions, instanceId uint64, value string) error {
  var nullid uint64 = 0
  state := common.AcceptorStateData{
    InstanceID:     &instanceId,
    AcceptedValue:  []byte(value),
    PromiseID:      &nullid,
    PromiseNodeID:  &nullid,
    AcceptedID:     &nullid,
    AcceptedNodeID: &nullid,
  }

  err := self.WriteState(options, instanceId, state)
  if err != nil {
    log.Error("WriteState to db fail, instanceid %d err:%v", instanceId, err)
    return err
  }

  return nil
}

func (self *PaxosLog) ReadLog(instanceId uint64) (string, error) {
  state, err := self.ReadState(instanceId)
  if err != nil {
    log.Error("readstate from db fail, instanceid %v, err %v",
      instanceId, err)
    return "", err
  }

  return string(state.GetAcceptedValue()), nil
}

func (self *PaxosLog) WriteState(options WriteOptions, instanceId uint64, state common.AcceptorStateData) error {
  buf, err := proto.Marshal(&state)
  if err != nil {
    log.Error("State serialize error:%v", err)
    return err
  }

  err = self.logStorage.Put(options, instanceId, buf)
  if err != nil {
    log.Error("DB.Put fail, bufferlen %d error %v", len(buf), err)
    return err
  }

  return nil
}

func (self *PaxosLog) ReadState(instanceId uint64)(*common.AcceptorStateData, error) {
  buf, err := self.logStorage.Get(instanceId)
  if err != nil {
    log.Error("DB.Get fail,error %v", err)
    return nil, err
  }
  if len(buf) == 0 {
    err = fmt.Errorf("DB.Get not found")
    return nil, err
  }

  var state common.AcceptorStateData
  err = proto.Unmarshal(buf, &state)
  if err != nil {
    log.Error("State Unseriasize fail: %v", err)
    return nil, err
  }

  return &state, nil
}

// common.ErrKeyNotFound or nil
func (self *PaxosLog) GetMaxInstanceIdFromLog()(uint64, error) {
  instanceId, err := self.logStorage.GetMaxInstanceID()
  if err != nil {
    log.Error("db.getmax fail, error:%v", err)
    return common.INVALID_INSTANCEID, err
  }

  return instanceId, nil
}

func NewPaxosLog(logStorage *LogStorage) *PaxosLog {
  return &PaxosLog{
    logStorage: logStorage,
  }
}
