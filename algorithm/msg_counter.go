package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "sync"
)

type MsgCounter struct {
  config *config.Config

  receiveMsgNodeIDMaps         map[uint64]bool
  rejectMsgNodeIDMaps          map[uint64]bool
  promiseOrAcceptMsgNodeIDMaps map[uint64]bool
  mutex sync.Mutex
}

func NewMsgCounter(config *config.Config) *MsgCounter {
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

func (self *MsgCounter) AddReceive(nodeId uint64) {
  self.mutex.Lock()
  self.receiveMsgNodeIDMaps[nodeId] = true
  self.mutex.Unlock()
}

func (self *MsgCounter) AddReject(nodeId uint64) {
  self.mutex.Lock()
  self.rejectMsgNodeIDMaps[nodeId] = true
  self.mutex.Unlock()
}

func (self *MsgCounter) AddPromiseOrAccept(nodeId uint64) {
  self.mutex.Lock()
  self.promiseOrAcceptMsgNodeIDMaps[nodeId] = true
  self.mutex.Unlock()
}

func (self *MsgCounter) IsPassedOnThisRound() bool {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  return len(self.promiseOrAcceptMsgNodeIDMaps) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) IsRejectedOnThisRound() bool {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  return len(self.rejectMsgNodeIDMaps) >= self.config.GetMajorityCount()
}

func (self *MsgCounter) IsAllReceiveOnThisRound() bool {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  return len(self.receiveMsgNodeIDMaps) == self.config.GetNodeCount()
}
