package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
)

type AcceptorState struct {
  promiseNum   BallotNumber
  acceptedNum  BallotNumber
  acceptValues []byte
  checkSum     uint32
  paxosLog     *logstorage.PaxosLog
  config       *config.Config
  syncTimes    int32
}

func newAcceptorState(config *config.Config, logStorage logstorage.LogStorage) *AcceptorState {
  acceptorState := &AcceptorState{
    config:    config,
    paxosLog:  logstorage.NewPaxosLog(logStorage),
    syncTimes: 0,
  }
  acceptorState.init()

  return acceptorState
}

func (self *AcceptorState) init() {
  self.acceptedNum.Reset()
  self.checkSum = 0
  self.acceptValues = []byte("")
}

func (self *AcceptorState) GetPromiseNum() BallotNumber {
  return self.promiseNum
}

func (self *AcceptorState) SetPromiseNum(promiseNum BallotNumber) {
  self.promiseNum = promiseNum
}

func (self *AcceptorState) GetAcceptedNum() BallotNumber {
  return self.acceptedNum
}

func (self *AcceptorState) SetAcceptedNum(acceptedNum BallotNumber) {
  self.acceptedNum = acceptedNum
}

func (self *AcceptorState) GetAcceptedValue() []byte {
  return self.acceptValues
}

func (self *AcceptorState) SetAcceptedValue(acceptedValue []byte) {
  self.acceptValues = acceptedValue
}

func (self *AcceptorState) GetChecksum() uint32 {
  return self.checkSum
}

func (self *AcceptorState) Persist(instanceid uint64, lastCheckSum uint32) error {
  if instanceid > 0 && lastCheckSum == 0 {
    self.checkSum = 0
  } else if len(self.acceptValues) > 0 {
    self.checkSum = util.Crc32(lastCheckSum, self.acceptValues, common.CRC32_SKIP)
  }

  var state = common.AcceptorStateData{
    InstanceID:     proto.Uint64(instanceid),
    PromiseID:      proto.Uint64(self.promiseNum.proposalId),
    PromiseNodeID:  proto.Uint64(self.promiseNum.nodeId),
    AcceptedID:     proto.Uint64(self.acceptedNum.proposalId),
    AcceptedNodeID: proto.Uint64(self.acceptedNum.nodeId),
    AcceptedValue:  self.acceptValues,
    Checksum:       proto.Uint32(self.checkSum),
  }

  var options = logstorage.WriteOptions{
    Sync: self.config.LogSync(),
  }

  if options.Sync {
    self.syncTimes++
    if self.syncTimes > self.config.SyncInterval() {
      self.syncTimes = 0
    } else {
      options.Sync = false
    }
  }

  err := self.paxosLog.WriteState(options, self.config.GetMyGroupIdx(), instanceid, state)
  if err != nil {
    return err
  }

  log.Info("Groupidx %d instanceid %d promiseid %d promisenodeid %d "+
    "acceptedid %d acceptednodeid %d valuelen %d cksum %d",
    self.config.GetMyGroupIdx, instanceid, self.promiseNum.proposalId,
    self.promiseNum.nodeId, self.acceptedNum.proposalId, self.acceptedNum.nodeId,
    len(self.acceptValues), self.checkSum)
  return nil
}

func (self *AcceptorState) Load(instanceid *uint64) error {
  groupId := self.config.GetMyGroupIdx()

  err := self.paxosLog.GetMaxInstanceIdFromLog(groupId, instanceid)
  if err != nil && err != common.ErrKeyNotFound {
    log.Info("Load max instance id fail:%v", err)
    return err
  }

  if err == common.ErrKeyNotFound {
    log.Info("empty database")
    *instanceid = 0
    return nil
  }

  var state common.AcceptorStateData
  err = self.paxosLog.ReadState(groupId, *instanceid, &state)
  if err != nil {
    return err
  }

  self.promiseNum.proposalId = state.GetPromiseID()
  self.promiseNum.nodeId = state.GetPromiseNodeID()
  self.acceptedNum.proposalId = state.GetAcceptedID()
  self.acceptedNum.nodeId = state.GetAcceptedNodeID()
  self.acceptValues = state.GetAcceptedValue()
  self.checkSum = state.GetChecksum()

  log.Info("Groupidx %d instanceid %d promiseid %d promisenodeid %d "+
    "acceptedid %d acceptednodeid %d valuelen %d cksum %d",
    self.config.GetMyGroupIdx, instanceid, self.promiseNum.proposalId,
    self.promiseNum.nodeId, self.acceptedNum.proposalId, self.acceptedNum.nodeId,
    len(self.acceptValues), self.checkSum)
  return nil
}
