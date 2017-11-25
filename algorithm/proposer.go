package algorithm

import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/util"

  log "github.com/lichuang/log4go"
)

type Proposer struct {
  Base

  config               *config.Config
  state                *ProposerState
  msgCounter           *MsgCounter
  learner              *Learner
  preparing            bool
  prepareTimerId       uint32
  acceptTimerId        uint32
  lastPrepareTimeoutMs uint32
  lastAcceptTimeoutMs  uint32
  canSkipPrepare       bool
  wasRejectBySomeone   bool
  timeoutInstanceId    uint64
  timerThread          *util.TimerThread
  //timeStat             *util.TimeStat
}

func NewProposer(instance *Instance) *Proposer {
  proposer := &Proposer{
    Base:        newBase(instance),
    config:      instance.config,
    state:       newProposalState(instance.config),
    msgCounter:  newMsgCounter(instance.config),
    learner:     instance.learner,
    timerThread: instance.timerThread,
    //timeStat: util.NewTimeStat(),
  }

  proposer.initForNewPaxosInstance()

  return proposer
}

func (self *Proposer) initForNewPaxosInstance() {
  self.msgCounter.StartNewRound()
  self.state.init()

  self.exitPrepare()
  self.exitAccept()
}

func (self *Proposer) NewInstance() {
  self.Base.newInstance()
  self.initForNewPaxosInstance()
}

func (self *Proposer) setStartProposalID(proposalId uint64) {
  self.state.setStartProposalId(proposalId)
}

func (self *Proposer) isWorking() bool {
  return self.prepareTimerId > 0 || self.acceptTimerId > 0
}

func (self *Proposer) newValue(value []byte) {
  if len(self.state.GetValue()) == 0 {
    self.state.SetValue(value)
  }

  self.lastPrepareTimeoutMs = common.GetStartPrepareTimeoutMs()
  self.lastAcceptTimeoutMs = common.GetStartAcceptTimeoutMs()

  if self.canSkipPrepare && !self.wasRejectBySomeone {
    log.Info("skip prepare,directly start accept")
    self.accept()
  } else {
    self.prepare(self.wasRejectBySomeone)
  }
}

func (self *Proposer) prepare(needNewBallot bool) {
  base := self.Base
  state := self.state

  log.Info("[%s]start now.instanceid %d mynodeid %d state.proposal id %d state.valuelen %d",
    self.instance.String(),base.GetInstanceId(), self.config.GetMyNodeId(), state.GetProposalId(), len(state.GetValue()))

  // first reset all state
  self.exitAccept()
  self.state.setState(PREPARE)
  self.canSkipPrepare = false
  self.wasRejectBySomeone = false
  self.state.ResetHighestOtherPreAcceptBallot()

  if needNewBallot {
    self.state.newPrepare()
  }

  // pack paxos prepare msg and broadcast
  msg := &common.PaxosMsg{
    MsgType:    proto.Int32(common.MsgType_PaxosPrepare),
    InstanceID: proto.Uint64(base.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(state.GetProposalId()),
  }

  self.msgCounter.StartNewRound()
  self.addPrepareTimer(1000)

  base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_First)
}

func (self *Proposer) exitAccept() {
  if self.acceptTimerId != 0 {
    self.timerThread.DelTimer(self.acceptTimerId)
    self.acceptTimerId = 0
  }
}

func (self *Proposer) exitPrepare() {
  if self.prepareTimerId != 0 {
    self.timerThread.DelTimer(self.prepareTimerId)
    self.prepareTimerId = 0
  }
}

func (self *Proposer) addPrepareTimer(timeOutMs uint32) {
  if self.prepareTimerId != 0 {
    self.timerThread.DelTimer(self.prepareTimerId)
    self.prepareTimerId = 0
  }

  now := util.NowTimeMs()
  self.prepareTimerId = self.timerThread.AddTimer(now + uint64(timeOutMs), PrepareTimer, self)
}

func (self *Proposer) addAcceptTimer(timeOutMs uint32) {
  if self.acceptTimerId != 0 {
    self.timerThread.DelTimer(self.acceptTimerId)
    self.acceptTimerId = 0
  }

  now := util.NowTimeMs()
  self.acceptTimerId = self.timerThread.AddTimer(now + uint64(timeOutMs), AcceptTimer, self)
}

func (self *Proposer) OnPrepareReply(msg *common.PaxosMsg) error {
  log.Info("[%s]START ", self.instance.String())

  if self.state.state != PREPARE {
    return nil
  }

  if msg.GetProposalID() != self.state.GetProposalId() {
    return nil
  }

  self.msgCounter.addReceive(msg.GetNodeID())

  if msg.GetRejectByPromiseID() == 0 {
    ballot := NewBallotNumber(msg.GetPreAcceptID(), msg.GetPreAcceptNodeID())
    self.msgCounter.AddPromiseOrAccept(msg.GetNodeID())
    self.state.AddPreAcceptValue(*ballot, msg.GetValue())
  } else {
    self.msgCounter.AddReject(msg.GetNodeID())
    self.wasRejectBySomeone = true
    self.state.SetOtherProposalId(msg.GetRejectByPromiseID())
  }

  if self.msgCounter.IsPassedOnThisRound() {
    self.canSkipPrepare = true
    self.accept()
  } else if (self.msgCounter.IsRejectedOnThisRound() || self.msgCounter.IsAllReceiveOnThisRound()){
    self.addPrepareTimer(40)
  }

  return nil
}

func (self *Proposer) accept() {
  log.Info("[%s]start accept", self.instance.String())

  self.exitAccept()
  self.state.setState(ACCEPT)

  base := self.Base
  state := self.state

  msg := &common.PaxosMsg{
    MsgType:    proto.Int32(common.MsgType_PaxosAccept),
    InstanceID: proto.Uint64(base.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(state.GetProposalId()),
    Value:state.GetValue(),
    LastChecksum:proto.Uint32(base.GetLastChecksum()),
  }

  self.msgCounter.StartNewRound()

  self.addAcceptTimer(100)

  base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_Final)
}

func (self *Proposer) OnAcceptReply(msg *common.PaxosMsg) error {
  state := self.state
  log.Info("[%s]START msg.proposalId %d, state.proposalId %d, msg.from %d, rejectby %d",
    self.instance.String(), msg.GetProposalID(), state.GetProposalId(), msg.GetNodeID(), msg.GetRejectByPromiseID())

  base := self.Base

  if state.state != ACCEPT {
    return nil
  }

  if msg.GetProposalID() != state.GetProposalId() {
    return nil
  }

  msgCounter := self.msgCounter
  if msg.GetRejectByPromiseID() == 0 {
    msgCounter.AddPromiseOrAccept(msg.GetNodeID())
  } else {
    msgCounter.AddReject(msg.GetNodeID())
    self.wasRejectBySomeone = true
    state.SetOtherProposalId(msg.GetRejectByPromiseID())
  }

  if msgCounter.IsPassedOnThisRound() {
    self.exitAccept()
    self.learner.ProposerSendSuccess(base.GetInstanceId(), state.GetProposalId())
    log.Info("[%s]instance %d passed", self.instance.String(), msg.GetInstanceID())
  } else {
    self.addAcceptTimer(30)
  }

  log.Info("OnAcceptReply END")
  return nil
}

func (self *Proposer)OnTimeout(timer *util.Timer) {
  log.Debug("[%s]proposer timeout type:%d", self.instance.String(),timer.TimerType)

}
