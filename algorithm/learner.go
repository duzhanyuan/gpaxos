package algorithm

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/storage"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
)

type Learner struct {
  Base
  instance                         *Instance
  paxosLog                         *storage.PaxosLog
  acceptor                         *Acceptor
  state                            *LearnerState
  isImLearning                     bool
  highestSeenInstanceID            uint64
  highestSeenInstanceID_fromNodeID uint64
  lastAckInstanceId                uint64
  askforlearnNoopTimerID           uint32
  sender                          *LearnerSender
  timerThread                     *util.TimerThread
}

func NewLearner(instance *Instance) *Learner {
  learner := &Learner{
    Base:                             newBase(instance),
    paxosLog:                         instance.paxosLog,
    acceptor:                         instance.acceptor,
    isImLearning:                     false,
    highestSeenInstanceID:            1,
    highestSeenInstanceID_fromNodeID: common.NULL_NODEID,
    lastAckInstanceId:                1,
    state:                            NewLearnerState(instance),
    instance:                         instance,
  }
  learner.sender = NewLearnerSender(instance, learner)

  learner.InitForNewPaxosInstance(false)

  return learner
}

func (self *Learner) InitForNewPaxosInstance(isMyCommit bool) {
  self.state.Init()
}

func (self *Learner) NewInstance(isMyComit bool) {
  self.Base.newInstance()
  self.InitForNewPaxosInstance(isMyComit)
  log.Debug("[%s]now learner instance id %d", self.instance.String(), self.Base.GetInstanceId())
}

func (self *Learner) Init() {
  self.sender.Start()
}

func (self *Learner) IsLearned() bool {
  return self.state.IsLearned()
}

func (self *Learner) GetLearnValue() []byte {
  return self.state.GetLearnValue()
}

func (self *Learner) Stop() {
  self.sender.Stop()
}

func (self *Learner) IsImLatest() bool {
  return self.GetInstanceId()+1 >= self.highestSeenInstanceID
}

func (self *Learner) GetSeenLatestInstanceID() uint64 {
  return self.highestSeenInstanceID
}

func (self *Learner) SetSeenInstanceID(instanceId uint64, fromNodeId uint64) {
  if instanceId > self.highestSeenInstanceID {
    self.highestSeenInstanceID = instanceId
    self.highestSeenInstanceID_fromNodeID = fromNodeId
  }
}

func (self *Learner) GetNewChecksum() uint32 {
  return self.state.GetNewChecksum()
}

func (self *Learner) Reset_AskforLearn_Noop(timeout uint32) {
  if self.askforlearnNoopTimerID > 0 {
    self.timerThread.DelTimer(self.askforlearnNoopTimerID)
  }

  self.askforlearnNoopTimerID = self.timerThread.AddTimer(timeout, LearnerTimer, self)
}

func (self *Learner) AskforLearn_Noop(isStart bool) {
  self.Reset_AskforLearn_Noop(common.GetAskforLearnInterval())
  self.isImLearning = false
  self.askforLearn()
}

func (self *Learner) askforLearn() {
  log.Info("start learn")

  base := self.Base

  msg := &common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    MsgType:    proto.Int32(common.MsgType_PaxosLearner_AskforLearn),
  }

  if self.config.IsIMFollower() {
    msg.ProposalNodeID = proto.Uint64(self.config.GetFollowToNodeID())
  }

  log.Info("end instanceid %d, mynodeid %d", msg.GetInstanceID(), msg.GetNodeID())

  base.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_None)
  //self.BroadcastMessageToTempNode(msg, common.Message_SendType_UDP)
}

func (self *Learner) OnAskforLearn(msg *common.PaxosMsg) {
  log.Info("start msg.instanceid %d now.instanceid %d msg.fromnodeid %d",
    msg.GetInstanceID(), self.GetInstanceId(), msg.GetNodeID())

  self.SetSeenInstanceID(msg.GetInstanceID(), msg.GetNodeID())
  if msg.GetProposalNodeID() == self.config.GetMyNodeId() {
    log.Info("found a node %d follow me", msg.GetNodeID())
    self.config.AddFollowerNode(msg.GetNodeID())
  }

  if msg.GetInstanceID() >= self.GetInstanceId() {
    return
  }

  /*
  if msg.GetInstanceID() >= self.CpMng.GetMinChosenInstanceID() {
    if !self.LearnerSender.Prepare(msg.GetInstanceID(), msg.GetNodeID()) {
      log.Error("learner sender working for others")

      if msg.GetInstanceID() == self.GetInstanceId()-1 {
        log.Info("instanceid only difference one, just send this value to other")
        var state common.AcceptorStateData
        err := self.PaxosLog.ReadState(self.config.GetMyGroupIdx(), msg.GetInstanceID(), &state)
        if err == nil {
          ballot := newBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())
          self.SendLearnValue(msg.GetNodeID(), msg.GetInstanceID(), ballot, state.GetAcceptedValue(), 0, false)
        }
      }

      return
    }
  }
  */

  self.sendNowInstanceID(msg.GetInstanceID(), msg.GetNodeID())
}

func (self *Learner) sendNowInstanceID(instanceId uint64, sendNodeId uint64) {
  msg := &common.PaxosMsg{
    InstanceID:          proto.Uint64(instanceId),
    NodeID:              proto.Uint64(self.config.GetMyNodeId()),
    MsgType:             proto.Int32(common.MsgType_PaxosLearner_SendNowInstanceID),
    NowInstanceID:       proto.Uint64(self.GetInstanceId()),
  }

  if self.GetInstanceId()-instanceId > 50 {
    /*
    systemVarBuffer, err := self.config.GetSystemVSM().GetCheckpointBuffer()
    if err == nil {
      msg.SystemVariables = util.CopyBytes(systemVarBuffer)
    }

    masterVarBuffer, err := self.config.GetMasterSM().GetCheckpointBuffer()
    if err == nil {
      msg.MasterVariables = util.CopyBytes(masterVarBuffer)
    }
    */
  }

  self.sendPaxosMessage(sendNodeId, msg)
}

func (self *Learner) SendLearnValue(sendNodeId uint64, learnInstanceId uint64,
  ballot BallotNumber, value []byte, cksum uint32, needAck bool) error {
  var paxosMsg = &common.PaxosMsg{
    MsgType:        proto.Int32(common.MsgType_PaxosLearner_SendLearnValue),
    InstanceID:     proto.Uint64(learnInstanceId),
    NodeID:         proto.Uint64(self.config.GetMyNodeId()),
    ProposalNodeID: proto.Uint64(ballot.nodeId),
    ProposalID:     proto.Uint64(ballot.proposalId),
    Value:          value,
    LastChecksum:   proto.Uint32(cksum),
  }

  if needAck {
    paxosMsg.Flag = proto.Uint32(common.PaxosMsgFlagType_SendLearnValue_NeedAck)
  }

  return self.sendPaxosMessage(sendNodeId, paxosMsg)
}

func (self *Learner) OnSendLearnValue(msg *common.PaxosMsg) {
  log.Info("START Msg.InstanceID %d Now.InstanceID %d Msg.ballot_proposalid %d Msg.ballot_nodeid %d Msg.ValueSize %d",
    msg.GetInstanceID(), self.GetInstanceId(), msg.GetProposalID(),
    msg.GetNodeID(), len(msg.Value))

  if msg.GetInstanceID() > self.GetInstanceId() {
    log.Debug("[Latest Msg] i can't learn")
    return
  }

  if msg.GetInstanceID() < self.GetInstanceId() {
    log.Debug("[Lag Msg] no need to learn")
  } else {
    ballot := NewBallotNumber(msg.GetProposalID(), msg.GetProposalNodeID())
    err := self.state.LearnValue(msg.GetInstanceID(), *ballot, msg.Value, self.GetLastChecksum())
    if err != nil {
      log.Error("LearnState.LearnValue fail:%v", err)
      return
    }
    log.Info("END LearnValue OK, proposalid %d proposalid_nodeid %d valueLen %d",
      msg.GetProposalID(), msg.GetNodeID(), len(msg.Value))
  }

  if msg.GetFlag() == common.PaxosMsgFlagType_SendLearnValue_NeedAck {
    self.Reset_AskforLearn_Noop(common.GetAskforLearnInterval())
    self.SendLearnValue_Ack(msg.GetNodeID())
  }
}

func (self *Learner) SendLearnValue_Ack(sendNodeId uint64) {
  log.Info("START LastAck.Instanceid %d Now.Instanceid %d", self.lastAckInstanceId, self.GetInstanceId())

  if self.GetInstanceId() < self.lastAckInstanceId+common.GetLeanerReceiver_Ack_Lead() {
    log.Info("no need ack")
    return
  }

  self.lastAckInstanceId = self.GetInstanceId()

  msg := &common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
    MsgType:    proto.Int32(common.MsgType_PaxosLearner_SendLearnValue_Ack),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
  }

  self.sendPaxosMessage(sendNodeId, msg)

  log.Info("END.OK")
}

func (self *Learner) OnSendLearnValue_Ack(msg *common.PaxosMsg) {
  log.Info("Msg.Ack.Instanceid %d Msg.from_nodeid %d", msg.GetInstanceID(), msg.GetNodeID())
  self.sender.Ack(msg.GetInstanceID(), msg.GetNodeID())
}

func (self *Learner) getSeenLatestInstanceId() uint64 {
  return 0
}

func (self *Learner) ProposerSendSuccess(instanceId uint64, proposalId uint64) {
  msg := &common.PaxosMsg{
    MsgType:      proto.Int32(common.MsgType_PaxosLearner_ProposerSendSuccess),
    InstanceID:   proto.Uint64(instanceId),
    NodeID:       proto.Uint64(self.config.GetMyNodeId()),
    ProposalID:   proto.Uint64(proposalId),
    LastChecksum: proto.Uint32(self.GetLastChecksum()),
  }

  self.broadcastMessage(msg, BroadcastMessage_Type_RunSelf_First)
}

func (self *Learner) OnProposerSendSuccess(msg *common.PaxosMsg) {
  log.Info("[%s]OnProposerSendSuccess Msg.InstanceID %d Now.InstanceID %d Msg.ProposalID %d "+
    "State.AcceptedID %d State.AcceptedNodeID %d, Msg.from_nodeid %d",
    self.instance.String(),msg.GetInstanceID(), self.GetInstanceId(), msg.GetProposalID(),
    self.acceptor.GetAcceptorState().acceptedNum.proposalId,
    self.acceptor.GetAcceptorState().acceptedNum.nodeId,
    msg.GetNodeID())

  if msg.GetInstanceID() != self.GetInstanceId() {
    log.Debug("instance id %d not same as msg instance id %d", self.GetInstanceId(), msg.GetInstanceID())
    return
  }

  if self.acceptor.GetAcceptorState().acceptedNum.IsNull() {
    log.Debug("not accepted any proposal")
    return
  }

  ballot := NewBallotNumber(msg.GetProposalID(), msg.GetNodeID())
  if !self.acceptor.GetAcceptorState().acceptedNum.EQ(ballot) {
    log.Debug("[%s]proposal ballot %s not same to accepted ballot %s", self.instance.String(),
      self.acceptor.GetAcceptorState().acceptedNum.String(),ballot.String())
    return
  }

  self.state.LearnValueWithoutWrite(msg.GetInstanceID(),
    self.acceptor.GetAcceptorState().GetAcceptedValue(),
    self.acceptor.GetAcceptorState().GetChecksum())

  log.Info("learn value instanceid %d ok", msg.GetInstanceID())
  //self.TransmitToFollower()
}

/*
func (self *Learner) TransmitToFollower() {
  if self.config.GetMyFollowerCount() == 0 {
    return
  }

  acceptor := self.acceptor
  msg := &common.PaxosMsg{
    MsgType:        proto.Int32(common.MsgType_PaxosLearner_SendLearnValue),
    InstanceID:     proto.Uint64(self.GetInstanceId()),
    NodeID:         proto.Uint64(self.config.GetMyNodeId()),
    ProposalNodeID: proto.Uint64(acceptor.GetAcceptorState().acceptedNum.nodeId),
    ProposalID:     proto.Uint64(acceptor.GetAcceptorState().acceptedNum.proposalId),
    Value:          acceptor.GetAcceptorState().GetAcceptedValue(),
    LastChecksum:   proto.Uint32(self.GetLastChecksum()),
  }

  self.broadcastMessageToFollower(msg)

  log.Info("OK")
}
*/

func (self *Learner)OnTimeout(timer *util.Timer) {
  log.Debug("learner timeout type:%d", timer.TimerType)
}
