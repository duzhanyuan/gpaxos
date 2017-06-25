package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/golang/protobuf/proto"
)

type Learner struct {
  Base
  PaxosLog                         *logstorage.PaxosLog
  Acceptor                         *Acceptor
  LearnerState                     *LearnerState
  IsImLearning                     bool
  HighestSeenInstanceID            uint64
  HighestSeenInstanceID_FromNodeID uint64
  LastAckInstanceId                uint64
  Askforlearn_noopTimerID          uint32
  SMFactory                        *sm_base.StateMachineFactory
  CpMng                            *checkpoint.CheckpointManager
  LearnerSender                    *LearnerSender
  CheckpointSender                 *CheckpointSender
  CheckpointReceiver               *CheckpointReceiver
  IOThread                         *IOThread
}

func NewLearner(config *config.Config, transport *common.MsgTransport,
  instance *Instance, acceptor *Acceptor,
  storage logstorage.LogStorage, thread *IOThread, manager *checkpoint.CheckpointManager,
  factory *sm_base.StateMachineFactory) *Learner {
  learner := &Learner{
    Base:                             newBase(config, transport, instance),
    PaxosLog:                         logstorage.NewPaxosLog(storage),
    Acceptor:                         acceptor,
    IsImLearning:                     false,
    HighestSeenInstanceID:            0,
    HighestSeenInstanceID_FromNodeID: common.NULL_NODEID,
    LastAckInstanceId:                0,
    CpMng:                            manager,
    SMFactory:                        factory,
    LearnerState:                     NewLearnerState(config, storage),
    IOThread:                         thread,
    CheckpointReceiver:               NewCheckpointReceiver(config, storage),
    CheckpointSender:                 nil,
  }
  learner.LearnerSender = NewLearnerSender(config, learner, storage)

  learner.InitForNewPaxosInstance()

  return learner
}

func (self *Learner) Init() {
  self.LearnerSender.Start()
}

func (self *Learner) IsLearned() bool {
  return self.LearnerState.GetIsLearned()
}

func (self *Learner) GetLearnValue() []byte {
  return self.LearnerState.GetLearnValue()
}

func (self *Learner) InitForNewPaxosInstance() {
  self.LearnerState.Init()
}

func (self *Learner) GetNewChecksum() uint32 {
  return self.LearnerState.GetNewChecksum()
}

func (self *Learner) Stop() {
  self.LearnerSender.Stop()
  if self.CheckpointSender != nil {
    self.CheckpointSender.Stop()
  }
}

func (self *Learner) IsIMLatest() bool {
  return self.GetInstanceId() + 1 >= self.HighestSeenInstanceID
}

func (self *Learner) GetSeenLatestInstanceID() uint64 {
  return self.HighestSeenInstanceID
}

func (self *Learner) SetSeenInstanceID(instanceId uint64, fromNodeId uint64) {
  if instanceId > self.HighestSeenInstanceID {
    self.HighestSeenInstanceID = instanceId
    self.HighestSeenInstanceID_FromNodeID = fromNodeId
  }
}

func (self *Learner) Reset_AskforLearn_Noop(timeout uint32) {
  if self.Askforlearn_noopTimerID > 0 {
    self.IOThread.RemoveTimer(&self.Askforlearn_noopTimerID)
  }

  self.Askforlearn_noopTimerID = self.IOThread.AddTimer(timeout, common.Timer_Learner_Askforlearn_noop)
}

func (self *Learner) AskforLearn_Noop(isStart bool) {
  self.Reset_AskforLearn_Noop(common.GetAskforLearnInterval())
  self.IsImLearning = false
  self.CpMng.ExitCheckpointMode()
  self.AskforLearn()
  if isStart {
    // TODO: AskforLearn twice?
    self.AskforLearn()
  }
}

func (self *Learner) AskforLearn() {
  log.Info("start learn")

  msg := common.PaxosMsg{
    InstanceID:proto.Uint64(self.GetInstanceId()),
    NodeID:proto.Uint64(self.config.GetMyNodeId()),
    MsgType:proto.Int32(common.MsgType_PaxosLearner_AskforLearn),
  }

  if self.config.IsIMFollower() {
    msg.ProposalNodeID = proto.Uint64(self.config.GetFollowToNodeID())
  }

  log.Info("end instanceid %d, mynodeid %d", msg.GetInstanceID(), msg.GetNodeID())

  self.BroadcastMessage(msg, BroadcastMessage_Type_RunSelf_None, common.Message_SendType_TCP)
  self.BroadcastMessageToTempNode(msg, common.Message_SendType_UDP)
}

func (self *Learner) OnAskforLearn(msg common.PaxosMsg) {
  log.Info("start msg.instanceid %d now.instanceid %d msg.fromnodeid %d minchoseninstanceid %d",
    msg.GetInstanceID(), self.GetInstanceId(), msg.GetNodeID(),
    self.CpMng.GetMinChosenInstanceID())

  self.SetSeenInstanceID(msg.GetInstanceID(), msg.GetNodeID())
  if msg.GetProposalNodeID() == self.config.GetMyNodeId() {
    log.Info("found a node %d follow me", msg.GetNodeID())
    self.config.AddFollowerNode(msg.GetNodeID())
  }

  if msg.GetInstanceID() >= self.GetInstanceId() {
    return
  }

  if msg.GetInstanceID() >= self.CpMng.GetMinChosenInstanceID() {
    if !self.LearnerSender.Prepare(msg.GetInstanceID(), msg.GetNodeID()) {
      log.Error("learner sender working for others"

      if msg.GetInstanceID() == self.GetInstanceId() - 1 {
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

  self.SendNowInstanceID(msg.GetInstanceID(), msg.GetNodeID())
}

func (self *Learner) SendNowInstanceID(instanceId uint64, sendNodeId uint64) {
  msg := common.PaxosMsg{
    InstanceID:proto.Uint64(instanceId),
    NodeID:proto.Uint64(self.config.GetMyNodeId()),
    MsgType:proto.Int32(common.MsgType_PaxosLearner_SendNowInstanceID),
    NowInstanceID:proto.Uint64(self.GetInstanceId()),
    MinChosenInstanceID:proto.Uint64(self.CpMng.GetMinChosenInstanceID())
  }

  if self.GetInstanceId() - instanceId > 50 {
    var systemVarBuffer []byte
    err := self.config.GetSystemVSM().GetCheckpointBuffer(&systemVarBuffer)
    if err == nil {
      msg.SystemVariables = systemVarBuffer
    }


  }
}

func (self *Learner) ProposerSendSuccess(instanceId uint64, proposerId uint64) {

}

func (self *Learner) SendLearnValue(sendNodeId uint64, learnInstanceId uint64,
  ballot BallotNumber, value []byte, cksum uint32, needAck bool) error {

}
