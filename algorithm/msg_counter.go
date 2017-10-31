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

func (self *MsgCounter) startNewRound() {
  self.receiveMsgNodeIDMaps = make(map[uint64]bool, 0)
  self.rejectMsgNodeIDMaps = make(map[uint64]bool, 0)
  self.promiseOrAcceptMsgNodeIDMaps = make(map[uint64]bool, 0)
}

func (self *MsgCounter) addReceive(nodeId uint64) {
  self.receiveMsgNodeIDMaps[nodeId] = true
}

func (self *MsgCounter) addReject(nodeId uint64) {
  self.rejectMsgNodeIDMaps[nodeId] = true
}

func (self *MsgCounter) addPromiseOrAccept(nodeId uint64) {
  self.promiseOrAcceptMsgNodeIDMaps[nodeId] = true
}

func (self *MsgCounter) isPassedOnThisRound() bool {
  return len(self.promiseOrAcceptMsgNodeIDMaps) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) isRejectedOnThisRound() bool {
  return len(self.rejectMsgNodeIDMaps) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) isAllReceiveOnThisRound() bool {
  return len(self.receiveMsgNodeIDMaps) == self.config.GetNodeCount()
}
