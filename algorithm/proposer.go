package algorithm

import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/log"
  "math/rand"
  "github.com/lichuang/gpaxos/util"
)

type Proposer struct {
  Base
  State                *ProposerState
  MsgCount             *MsgCounter
  Learner              *Learner
  IOThread             *IOThread
  IsPreparing          bool
  PrepareTimerId       uint32
  IsAccepting          bool
  AcceptTimerId        uint32
  LastPrepareTimeoutMs uint32
  LastAcceptTimeoutMs  uint32
  CanSkipPrepare       bool
  WasRejectBySomeone   bool
  TimeoutInstanceId    uint64
  TimeStat             *util.TimeStat
}

func NewProposer(config *config.Config, transport common.MsgTransport,
  instance *Instance, learner *Learner, thread *IOThread) *Proposer {
  proposer := &Proposer{
    Base:     newBase(config, transport, instance),
    State:    NewProposalState(config),
    MsgCount: NewMsgCounter(config),
    Learner:  learner,
    IOThread: thread,
    TimeStat: util.NewTimeStat(),
  }

  proposer.initForNewPaxosInstance()

  return proposer
}

func (self *Proposer) SetStartProposalID(proposalId uint64) {
  self.State.SetStartProposalId(proposalId)
}

func (self *Proposer) IsWodking() bool {
  return self.IsPreparing || self.IsAccepting
}

func (self *Proposer) NewValue(value []byte) {
  if len(self.State.GetValue()) == 0 {
    self.State.SetValue(value)
  }

  self.LastPrepareTimeoutMs = common.GetStartPrepareTimeoutMs()
  self.LastAcceptTimeoutMs = common.GetStartAcceptTimeoutMs()

  if self.CanSkipPrepare && !self.WasRejectBySomeone {
    log.Info("skip prepare,directly start accept")
    self.Accept()
  } else {
    self.Prepare(self.WasRejectBySomeone)
  }
}

func (self *Proposer) AddPrepareTimer(timeoutMs uint32) {
  if self.PrepareTimerId > 0 {
    self.IOThread.RemoveTimer(&self.PrepareTimerId)
  }

  if timeoutMs > 0 {
    self.PrepareTimerId = self.IOThread.AddTimer(timeoutMs, common.Timer_Proposer_Prepare_Timeout)
    return
  }

  self.PrepareTimerId = self.IOThread.AddTimer(self.LastPrepareTimeoutMs, common.Timer_Proposer_Prepare_Timeout)
  self.TimeoutInstanceId = self.GetInstanceId()

  log.Info("prepare timeoutms:%d", self.LastPrepareTimeoutMs)
  self.LastPrepareTimeoutMs *= 2
  if self.LastPrepareTimeoutMs > common.GetMaxPrepareTimeoutMs() {
    self.LastPrepareTimeoutMs = common.GetMaxPrepareTimeoutMs()
  }
}

func (self *Proposer) AddAcceptTimer(timeoutMs uint32) {
  if self.AcceptTimerId > 0 {
    self.IOThread.RemoveTimer(&self.AcceptTimerId)
  }

  if timeoutMs > 0 {
    self.AcceptTimerId = self.IOThread.AddTimer(timeoutMs, common.Timer_Proposer_Accept_Timeout)
    return
  }

  self.AcceptTimerId = self.IOThread.AddTimer(self.LastPrepareTimeoutMs, common.Timer_Proposer_Accept_Timeout)
  self.TimeoutInstanceId = self.GetInstanceId()

  log.Info("accept timeoutms:%d", self.LastAcceptTimeoutMs)
  self.LastAcceptTimeoutMs *= 2
  if self.LastAcceptTimeoutMs > common.GetMaxAcceptTimeoutMs() {
    self.LastAcceptTimeoutMs = common.GetMaxAcceptTimeoutMs()
  }
}

func (self *Proposer) Prepare(needNewBallot bool) {
  log.Info("start now.instanceid %d mynodeid %d state.proposal id %d state.valuelen %d",
    self.GetInstanceId(), self.config.GetMyNodeId(), self.State.GetProposalId(), len(self.State.Value))

  self.TimeStat.Point()

  self.exitAccept()
  self.IsPreparing = true
  self.CanSkipPrepare = false
  self.WasRejectBySomeone = false

  self.State.ResetHighestOtherPreAcceptBallot()

  if needNewBallot {
    self.State.NewPrepare()
  }

  msg := common.PaxosMsg{
    MsgType:    proto.Int32(common.MsgType_PaxosPrepare),
    InstanceID: proto.Uint64(self.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    ProposalID: proto.Uint64(self.State.GetProposalId()),
  }

  self.MsgCount.StartNewRound()

  self.BroadcastMessage(msg, BroadcastMessage_Type_RunSelf_First, common.Message_SendType_UDP)
}

func (self *Proposer) OnPrepareReply(msg *common.PaxosMsg) {
  log.Info("start msg.proposal id %d state.proposal id %d msg.from nodeid %d rejectbypromiseid %d",
    msg.GetProposalID(), self.State.GetProposalId(), msg.GetNodeID(), msg.GetRejectByPromiseID())

  if !self.IsPreparing {
    return
  }

  if self.State.ProposalId != msg.GetProposalID() {
    return
  }

  self.MsgCount.AddReceive(msg.GetNodeID())

  if msg.GetRejectByPromiseID() == 0 {
    ballot := newBallotNumber(msg.GetPreAcceptID(), msg.GetPreAcceptNodeID())
    self.MsgCount.AddPromiseOrAccept(msg.GetNodeID())
    self.State.AddPreAcceptValue(ballot, msg.GetValue())
    log.Debug("[promise]preacceptid %d， preacceptnodeid %d valuesize %d",
      msg.GetPreAcceptID(), msg.GetPreAcceptNodeID(), len(msg.GetValue()))
  } else {
    self.MsgCount.AddReject(msg.GetNodeID())
    self.WasRejectBySomeone = true
    self.State.SetOtherProposalId(msg.GetRejectByPromiseID())
    log.Debug("[reject] reject by promise id %d", msg.GetRejectByPromiseID())
  }

  if self.MsgCount.IsPassedOnThisRound() {
    useTime := self.TimeStat.Point()
    log.Info("[pass]start accept, use time %d ms", useTime)
    self.CanSkipPrepare = true
    self.Accept()
  } else if self.MsgCount.IsRejectedOnThisRound() || self.MsgCount.IsAllReceiveOnThisRound() {
    log.Info("[not pass]wait 30ms and restart prepare")
    self.AddPrepareTimer(10 + rand.Uint32()%30)
  }
}

func (self *Proposer) Accept() {
  state := self.State
  log.Info("start proposerid %d valuesize %d", state.ProposalId, len(state.Value))

  self.TimeStat.Point()

  self.exitPrepare()
  self.IsAccepting = true

  msg := common.PaxosMsg{
    MsgType:      proto.Int32(common.MsgType_PaxosAccept),
    InstanceID:   proto.Uint64(self.GetInstanceId()),
    NodeID:       proto.Uint64(self.config.GetMyNodeId()),
    ProposalID:   proto.Uint64(state.ProposalId),
    Value:        state.Value,
    LastChecksum: proto(self.GetLastChecksum()),
  }

  self.MsgCount.StartNewRound()

  self.AddAcceptTimer(0)

  self.BroadcastMessage(msg, BroadcastMessage_Type_RunSelf_Final, common.Message_SendType_UDP)
}

func (self *Proposer) OnAcceptReply(msg *common.PaxosMsg) {
  state := self.State
  log.Info("start msg.proposerid %d state.proposerid %d msg.from_nodeid %d reject by promise id %d",
    msg.GetProposalID(), state.ProposalId, msg.GetNodeID(), msg.GetRejectByPromiseID())

  if !self.IsAccepting {
    return
  }

  if msg.GetProposalID() != state.ProposalId {
    return
  }

  self.MsgCount.AddReceive(msg.GetNodeID())

  if msg.GetRejectByPromiseID() == 0 {
    log.Debug("[Accept]")
    self.MsgCount.AddPromiseOrAccept(msg.GetNodeID())
  } else {
    log.Debug("[Reject]")
    self.MsgCount.AddReject(msg.GetNodeID())
    self.WasRejectBySomeone = true
    state.SetOtherProposalId(msg.GetRejectByPromiseID())
  }

  if self.MsgCount.IsPassedOnThisRound() {
    useTime := self.TimeStat.Point()
    log.Info("[pass]start send learn, usetime %d ms", useTime)
    self.exitAccept()
  } else if self.MsgCount.IsRejectedOnThisRound() || self.MsgCount.IsAllReceiveOnThisRound() {
    log.Info("[not pass]wait 30ms and restart prepare")
    self.AddAcceptTimer(rand.Uint32()%30 + 10)
  }
}

func (self *Proposer) OnPrepareTimeout() {
  log.Info("OnPrepareTimeout")

  if self.GetInstanceId() != self.TimeoutInstanceId {
    log.Error("timeoutinstanceid %d not same to nowinstanceid %d",
      self.TimeoutInstanceId, self.GetInstanceId())
    return
  }

  self.Prepare(self.WasRejectBySomeone)
}

func (self *Proposer) OnAcceptTimeout() {
  log.Info("OnAcceptTimeout")

  if self.GetInstanceId() != self.TimeoutInstanceId {
    log.Error("timeoutinstanceid %d not same to nowinstanceid %d",
      self.TimeoutInstanceId, self.GetInstanceId())
    return
  }

  self.Prepare(self.WasRejectBySomeone)
}

func (self *Proposer) initForNewPaxosInstance() {
  self.MsgCount.StartNewRound()
  self.State.init()

}

func (self *Proposer) exitPrepare() {
  if self.IsPreparing {
    self.IsPreparing = false

    self.IOThread.RemoveTimer(&self.PrepareTimerId)
  }
}

func (self *Proposer) exitAccept() {
  if self.IsAccepting {
    self.IsAccepting = false
    self.IOThread.RemoveTimer(&self.AcceptTimerId)
  }
}

func (self *Proposer) CancelSkipPrepare() {
  self.CanSkipPrepare = false
}
