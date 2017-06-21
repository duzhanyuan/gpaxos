package algorithm

import (
  "github.com/lichuang/gpaxos/config"
)

type MsgCounter struct {
  config *config.Config

  ReceiveMsgNodeIDMap         map[uint64]bool
  RejectMsgNodeIDMap          map[uint64]bool
  PromiseOrAcceptMsgNodeIDMap map[uint64]bool
}

func NewMsgCounter(config *config.Config) *MsgCounter {
  counter := &MsgCounter{
    config: config,
  }

  return counter
}

func (self *MsgCounter) StartNewRound() {
  self.ReceiveMsgNodeIDMap = make(map[uint64]bool, 0)
  self.RejectMsgNodeIDMap = make(map[uint64]bool, 0)
  self.PromiseOrAcceptMsgNodeIDMap = make(map[uint64]bool, 0)
}

func (self *MsgCounter) AddReceive(nodeId uint64) {
  self.ReceiveMsgNodeIDMap[nodeId] = true
}

func (self *MsgCounter) AddReject(nodeId uint64) {
  self.RejectMsgNodeIDMap[nodeId] = true
}

func (self *MsgCounter) AddPromiseOrAccept(nodeId uint64) {
  self.PromiseOrAcceptMsgNodeIDMap[nodeId] = true
}

func (self *MsgCounter) IsPassedOnThisRound() bool {
  return len(self.PromiseOrAcceptMsgNodeIDMap) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) IsRejectedOnThisRound() bool {
  return len(self.RejectMsgNodeIDMap) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) IsAllReceiveOnThisRound() bool {
  return len(self.ReceiveMsgNodeIDMap) == self.config.GetNodeCount()
}
