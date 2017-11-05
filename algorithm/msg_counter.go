package algorithm

import (
  "github.com/lichuang/gpaxos/config"
)

type MsgCounter struct {
  config *config.Config

  receiveMsgNodeIDMaps         map[uint64]bool
  rejectMsgNodeIDMaps          map[uint64]bool
  promiseOrAcceptMsgNodeIDMaps map[uint64]bool
}

func newMsgCounter(config *config.Config) *MsgCounter {
  counter := &MsgCounter{
    config: config,
  }

  return counter
}

func (self *MsgCounter) StartNewRound() {
  self.receiveMsgNodeIDMaps = make(map[uint64]bool, 0)
  self.rejectMsgNodeIDMaps = make(map[uint64]bool, 0)
  self.promiseOrAcceptMsgNodeIDMaps = make(map[uint64]bool, 0)
}

func (self *MsgCounter) addReceive(nodeId uint64) {
  self.receiveMsgNodeIDMaps[nodeId] = true
}

func (self *MsgCounter) AddReject(nodeId uint64) {
  self.rejectMsgNodeIDMaps[nodeId] = true
}

func (self *MsgCounter) AddPromiseOrAccept(nodeId uint64) {
  self.promiseOrAcceptMsgNodeIDMaps[nodeId] = true
}

func (self *MsgCounter) IsPassedOnThisRound() bool {
  return len(self.promiseOrAcceptMsgNodeIDMaps) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) IsRejectedOnThisRound() bool {
  return len(self.rejectMsgNodeIDMaps) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) IsAllReceiveOnThisRound() bool {
  return len(self.receiveMsgNodeIDMaps) == self.config.GetNodeCount()
}
