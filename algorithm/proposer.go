package algorithm

import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/util"

  log "github.com/lichuang/log4go"
)

type Proposer struct {
  base                 *Base
  config               *config.Config
  state                *ProposerState
  msgCount             *MsgCounter
  learner              *Learner
  preparing            bool
  prepareTimerId       uint32
  acceptTimerId        uint32
  lastPrepareTimeoutMs uint32
  lastAcceptTimeoutMs  uint32
  canSkipPrepare       bool
  wasRejectBySomeone   bool
  timeoutInstanceId    uint64
  timerThread         *util.TimerThread
  //timeStat             *util.TimeStat
}

func newProposer(instance *Instance) *Proposer {
  proposer := &Proposer{
    base:     newBase(instance),
    config:   instance.config,
    state:    newProposalState(instance.config),
    msgCount: newMsgCounter(instance.config),
    learner: instance.learner,
    timerThread:instance.timerThread,
    //timeStat: util.NewTimeStat(),
  }

  proposer.initForNewPaxosInstance()

  return proposer
}

func (self *Proposer) initForNewPaxosInstance() {
  self.msgCount.startNewRound()
  self.state.init()
}

func (self *Proposer) setStartProposalID(proposalId uint64) {
  self.state.setStartProposalId(proposalId)
}

func (self *Proposer) isWorking() bool {
  return self.prepareTimerId > 0 || self.acceptTimerId > 0
}

func (self *Proposer) newValue(value []byte) {
  if len(self.state.getValue()) == 0 {
    self.state.setValue(value)
  }

  self.lastPrepareTimeoutMs = common.GetStartPrepareTimeoutMs()
  self.lastAcceptTimeoutMs = common.GetStartAcceptTimeoutMs()

  if self.canSkipPrepare && !self.wasRejectBySomeone {
    log.Info("skip prepare,directly start accept")
    //self.Accept()
  } else {
    self.prepare(self.wasRejectBySomeone)
  }
}

func (self *Proposer) prepare(needNewBallot bool) {
  base := self.base
  state := self.state

  log.Info("start now.instanceid %d mynodeid %d state.proposal id %d state.valuelen %d",
    base.getInstanceId(), self.config.GetMyNodeId(), state.getProposalId(), len(state.getValue()))

  // first reset all state
  self.exitAcceptState()
  self.state.setState(PREPARE)
  self.canSkipPrepare = false
  self.wasRejectBySomeone = false
  self.state.resetHighestOtherPreAcceptBallot()

  if needNewBallot {
    self.state.newPrepare()
  }

  // pack paxos prepare msg and broadcast
  msg := &common.PaxosMsg{
    MsgType:    proto.Int32(common.MsgType_PaxosPrepare),
    InstanceID: proto.Uint64(base.getInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(state.getProposalId()),
  }

  self.msgCount.startNewRound()

  base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_First)
}

func (self *Proposer) exitAcceptState() {
  if self.acceptTimerId != 0 {
    self.timerThread.DelTimer(self.acceptTimerId)
    self.acceptTimerId = 0
  }
}

func (self *Proposer) getInstanceId() uint64 {
  return self.base.getInstanceId()
}
