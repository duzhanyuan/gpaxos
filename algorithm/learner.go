package algorithm

import (
  "errors"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos"
  "os"
  "github.com/lichuang/gpaxos/util"
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

func NewLearner(config *config.Config, transport common.MsgTransport,
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
  return self.GetInstanceId()+1 >= self.HighestSeenInstanceID
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
    InstanceID: proto.Uint64(self.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    MsgType:    proto.Int32(common.MsgType_PaxosLearner_AskforLearn),
  }

  if self.config.IsIMFollower() {
    msg.ProposalNodeID = proto.Uint64(self.config.GetFollowToNodeID())
  }

  log.Info("end instanceid %d, mynodeid %d", msg.GetInstanceID(), msg.GetNodeID())

  self.BroadcastMessage(msg, BroadcastMessage_Type_RunSelf_None, common.Message_SendType_TCP)
  self.BroadcastMessageToTempNode(msg, common.Message_SendType_UDP)
}

func (self *Learner) OnAskforLearn(msg *common.PaxosMsg) {
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

  self.SendNowInstanceID(msg.GetInstanceID(), msg.GetNodeID())
}

func (self *Learner) SendNowInstanceID(instanceId uint64, sendNodeId uint64) {
  msg := common.PaxosMsg{
    InstanceID:          proto.Uint64(instanceId),
    NodeID:              proto.Uint64(self.config.GetMyNodeId()),
    MsgType:             proto.Int32(common.MsgType_PaxosLearner_SendNowInstanceID),
    NowInstanceID:       proto.Uint64(self.GetInstanceId()),
    MinChosenInstanceID: proto.Uint64(self.CpMng.GetMinChosenInstanceID()),
  }

  if self.GetInstanceId()-instanceId > 50 {
    var systemVarBuffer []byte
    err := self.config.GetSystemVSM().GetCheckpointBuffer(systemVarBuffer)
    if err == nil {
      msg.SystemVariables = systemVarBuffer
    }

  }
}

func (self *Learner) SendLearnValue(sendNodeId uint64, learnInstanceId uint64,
  ballot BallotNumber, value []byte, cksum uint32, needAck bool) error {
  var paxosMsg = common.PaxosMsg{
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

  return self.SendPaxosMessage(sendNodeId, paxosMsg, common.Message_SendType_TCP)
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
    ballot := newBallotNumber(msg.GetProposalID(), msg.GetProposalNodeID())
    err := self.LearnerState.LearnValue(msg.GetInstanceID(), ballot, msg.Value, self.GetLastChecksum())
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
  log.Info("START LastAck.Instanceid %d Now.Instanceid %d", self.LastAckInstanceId, self.GetInstanceId())

  if self.GetInstanceId() < self.LastAckInstanceId+common.GetLeanerReceiver_Ack_Lead() {
    log.Info("no need ack")
    return
  }

  self.LastAckInstanceId = self.GetInstanceId()

  msg := common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
    MsgType:    proto.Int32(common.MsgType_PaxosLearner_SendLearnValue_Ack),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
  }

  self.SendPaxosMessage(sendNodeId, msg, common.Message_SendType_UDP)
}

func (self *Learner) OnSendLearnValue_Ack(msg *common.PaxosMsg) {
  log.Info("Msg.Ack.Instanceid %d Msg.from_nodeid %d", msg.GetInstanceID(), msg.GetNodeID())
  self.LearnerSender.Ack(msg.GetInstanceID(), msg.GetNodeID())
}

func (self *Learner) TransmitToFollower() {
  if self.config.GetFollowToNodeID() == 0 {
    return
  }

  msg := common.PaxosMsg{
    MsgType:        proto.Int32(common.MsgType_PaxosLearner_SendLearnValue),
    InstanceID:     proto.Uint64(self.GetInstanceId()),
    NodeID:         proto.Uint64(self.config.GetMyNodeId()),
    ProposalNodeID: proto.Uint64(self.Acceptor.GetAcceptorState().AcceptedNum.nodeId),
    ProposalID:     proto.Uint64(self.Acceptor.GetAcceptorState().AcceptedNum.proposalId),
    Value:          self.Acceptor.GetAcceptorState().GetAcceptedValue(),
    LastChecksum:   proto.Uint32(self.GetLastChecksum()),
  }

  self.BroadcastMessageToFollower(msg, common.Message_SendType_TCP)
}

func (self *Learner) ProposerSendSuccess(learnInstanceId uint64, proposerId uint64) {

  msg := common.PaxosMsg{
    MsgType:      proto.Int32(common.MsgType_PaxosLearner_ProposerSendSuccess),
    InstanceID:   proto.Uint64(learnInstanceId),
    NodeID:       proto.Uint64(self.config.GetMyNodeId()),
    ProposalID:   proto.Uint64(proposerId),
    LastChecksum: proto.Uint32(self.GetLastChecksum()),
  }

  self.BroadcastMessage(msg, BroadcastMessage_Type_RunSelf_First, common.Message_SendType_UDP)
}

func (self *Learner) OnProposerSendSuccess(msg *common.PaxosMsg) {
  log.Info("START Msg.InstanceID %d Now.InstanceID %d Msg.ProposalID %d "+
    "State.AcceptedID %d State.AcceptedNodeID %d, Msg.from_nodeid %d",
    msg.GetInstanceID(), self.GetInstanceId(), msg.GetProposalID(),
    self.Acceptor.GetAcceptorState().AcceptedNum.proposalId,
    self.Acceptor.GetAcceptorState().AcceptedNum.nodeId,
    msg.GetNodeID())

  if msg.GetInstanceID() != self.GetInstanceId() {
    return
  }

  if self.Acceptor.GetAcceptorState().AcceptedNum.IsNull() {
    log.Debug("not accepted any proposal")
    return
  }

  ballot := newBallotNumber(msg.GetProposalID(), msg.GetProposalNodeID())
  if !self.Acceptor.GetAcceptorState().AcceptedNum.EQ(ballot) {
    log.Debug("proposal ballot not same to accepted ballot")
    return
  }

  self.LearnerState.LearnValueWithoutWrite(msg.GetInstanceID(),
    self.Acceptor.GetAcceptorState().GetAcceptedValue(),
    self.Acceptor.GetAcceptorState().GetChecksum())

  log.Info("end learn value ok")
  self.TransmitToFollower()
}

func (self *Learner) AskforCheckpoint(sendNodeId uint64) {
  err := self.CpMng.PrepareForAskforCheckpoint(sendNodeId)
  if err != nil {
    return
  }

  msg := common.PaxosMsg{
    InstanceID: proto.Uint64(self.GetInstanceId()),
    NodeID:     proto.Uint64(self.config.GetMyNodeId()),
    MsgType:    proto.Int32(common.MsgType_PaxosLearner_AskforCheckpoint),
  }

  self.SendPaxosMessage(sendNodeId, msg, common.Message_SendType_UDP)
}

func (self *Learner) OnAskforCheckpoint(msg *common.PaxosMsg) {
  ckSender := self.GetNewCheckpointSender(msg.GetNodeID())
  if ckSender != nil {
    ckSender.Start()
    log.Info("new checkpoint sender started, send to nodeid %d", msg.GetNodeID())
  } else {
    log.Error("checkpoint sender is running")
  }
}

func (self *Learner) SendCheckpointBegin(sendNodeId uint64, uuid uint64, sequence uint64, ckInstanceId uint64) error {
  msg := common.CheckpointMsg{
    MsgType:              proto.Int32(common.CheckpointMsgType_SendFile),
    NodeID:               proto.Uint64(self.config.GetMyNodeId()),
    Flag:                 proto.Int32(common.CheckpointSendFileFlag_BEGIN),
    UUID:                 proto.Uint64(uuid),
    Sequence:             proto.Uint64(sequence),
    CheckpointInstanceID: proto.Uint64(ckInstanceId),
  }

  log.Info("[begin]SendNodeID %d uuid %d sequence %d cpi %d",
    sendNodeId, uuid, sequence, ckInstanceId)

  return self.SendCheckpointMessage(sendNodeId, msg, common.Message_SendType_TCP)
}

func (self *Learner) SendCheckpointEnd(sendNodeId uint64, uuid uint64, sequence uint64, ckInstanceId uint64) error {
  msg := common.CheckpointMsg{
    MsgType:              proto.Int32(common.CheckpointMsgType_SendFile),
    NodeID:               proto.Uint64(self.config.GetMyNodeId()),
    Flag:                 proto.Int32(common.CheckpointSendFileFlag_END),
    UUID:                 proto.Uint64(uuid),
    Sequence:             proto.Uint64(sequence),
    CheckpointInstanceID: proto.Uint64(ckInstanceId),
  }

  log.Info("[end]SendNodeID %d uuid %d sequence %d cpi %d",
    sendNodeId, uuid, sequence, ckInstanceId)

  return self.SendCheckpointMessage(sendNodeId, msg, common.Message_SendType_TCP)
}

func (self *Learner) SendCheckpoint(sendNodeId uint64, uuid uint64, sequence uint64,
  ckInstanceId uint64, cksum uint32, filePath string,
  smid int32, offset uint64, buffer []byte) error {
  msg := common.CheckpointMsg{
    MsgType:              proto.Int32(common.CheckpointMsgType_SendFile),
    NodeID:               proto.Uint64(self.config.GetMyNodeId()),
    Flag:                 proto.Int32(common.CheckpointSendFileFlag_ING),
    UUID:                 proto.Uint64(uuid),
    Sequence:             proto.Uint64(sequence),
    CheckpointInstanceID: proto.Uint64(ckInstanceId),
    Checksum:             proto.Uint32(cksum),
    FilePath:             proto.String(filePath),
    SMID:                 proto.Int32(smid),
    Offset:               proto.Uint64(offset),
    Buffer:               buffer,
  }

  log.Info("[end]SendNodeID %d uuid %d sequence %d cpi %d cksum %d smid %d offset %d "+
    "buffsersize %d filepath %s",
    sendNodeId, uuid, sequence, ckInstanceId, cksum, smid, offset, len(buffer), filePath)

  return self.SendCheckpointMessage(sendNodeId, msg, common.Message_SendType_TCP)
}

func (self *Learner) OnSendCheckpoint_Begin(ckmsg common.CheckpointMsg) error {
  err := self.CheckpointReceiver.NewReceiver(ckmsg.GetNodeID(), ckmsg.GetUUID())
  if err == nil {
    log.Info("new receiver ok")
    err = self.CpMng.SetMinChosenInstanceID(ckmsg.GetCheckpointInstanceID())
    if err != nil {
      log.Error("SetMinChosenInstanceID fail %v CheckpointInstanceID %d", err, ckmsg.GetCheckpointInstanceID())
      return err
    }
  }

  return err
}

func (self *Learner) OnSendCheckpoint_Ing(msg common.CheckpointMsg) error {
  return self.CheckpointReceiver.ReceiveCheckpoint(msg)
}

func (self *Learner) OnSendCheckpoint_End(msg common.CheckpointMsg) error {
  if !self.CheckpointReceiver.IsReceiverFinish(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence()) {
    log.Error("receive end msg but receiver not finish")
    return errors.New("")
  }

  stateMachines := self.SMFactory.GetStateMachines()
  for _, statemachine := range stateMachines {
    smid := statemachine.SMID()
    if smid == gpaxos.MASTER_V_SMID || smid == gpaxos.SYSTEM_V_SMID {
      continue
    }

    tmpDirPath := self.CheckpointReceiver.GetTmpDirPath(smid)
    filePathList := make([]string, 0)

    err := util.IterDir(tmpDirPath, filePathList)
    if err != nil {
      log.Error("IterDir fail %v dirpath %s", err, tmpDirPath)
    }

    if len(filePathList) == 0 {
      continue
    }

    err = statemachine.LoadCheckpointState(self.config.GetMyGroupIdx(),
      tmpDirPath, filePathList, msg.GetCheckpointInstanceID())
    if err != nil {
      return err
    }
  }

  log.Info("All sm load state ok, start to exit process")
  os.Exit(-1)

  return nil
}

func (self *Learner) OnSendCheckpoint(msg common.CheckpointMsg) {
  log.Info("START uuid %d flag %d sequence %d cpi %d checksum %u smid %d offset %d buffsize %zu filepath %s",
    msg.GetUUID(), msg.GetFlag(), msg.GetSequence(), msg.GetCheckpointInstanceID(),
    msg.GetChecksum(), msg.GetSMID(), msg.GetOffset(), len(msg.Buffer), msg.GetFilePath())

  var err error
  flag := msg.GetFlag()

  if flag == common.CheckpointSendFileFlag_BEGIN {
    err = self.OnSendCheckpoint_Begin(msg)
  } else if flag == common.CheckpointSendFileFlag_ING {
    err = self.OnSendCheckpoint_Ing(msg)
  } else if flag == common.CheckpointSendFileFlag_END {
    err = self.OnSendCheckpoint_End(msg)
  }

  if err != nil {
    log.Error("[FAIL] reset checkpoint receiver and reset askforlearn")
    self.CheckpointReceiver.Reset()

    self.Reset_AskforLearn_Noop(5000)
    self.SendCheckpointAck(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence(), common.CheckpointSendFileAckFlag_Fail)
  } else {
    self.SendCheckpointAck(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence(), common.CheckpointSendFileAckFlag_OK)
    self.Reset_AskforLearn_Noop(12000)
  }
}

func (self *Learner) SendCheckpointAck(sendNodeId uint64, uuid uint64, sequence uint64, flag int32) error {
  msg := common.CheckpointMsg{
    MsgType:  proto.Int32(common.CheckpointMsgType_SendFile_Ack),
    NodeID:   proto.Uint64(self.config.GetMyNodeId()),
    Flag:     proto.Int32(flag),
    UUID:     proto.Uint64(uuid),
    Sequence: proto.Uint64(sequence),
  }
  return self.SendCheckpointMessage(sendNodeId, msg, common.Message_SendType_TCP)
}

func (self *Learner) OnSendCheckpointAck(msg *common.CheckpointMsg) {
  log.Info("START flag %d", msg.GetFlag())

  if self.CheckpointSender != nil && !self.CheckpointSender.IsEnd() {
    if msg.GetFlag() == common.CheckpointSendFileAckFlag_OK {
      self.CheckpointSender.Ack(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence())
    } else {
      self.CheckpointSender.End()
    }
  }
}

func (self *Learner) GetNewCheckpointSender(sendNodeId uint64) *CheckpointSender {
  if self.CheckpointSender != nil {
    if self.CheckpointSender.IsEnd() {
      self.CheckpointSender.Stop()
      self.CheckpointSender = nil
    }
  }

  if self.CheckpointSender == nil {
    self.CheckpointSender = NewCheckpointSender(sendNodeId, self.config, self, self.SMFactory, self.CpMng)
    return self.CheckpointSender
  }

  return nil
}

func (self *Learner) OnSendNowInstanceID(msg *common.PaxosMsg) {
  log.Info("START Msg.InstanceID %d Now.InstanceID %d Msg.from_nodeid %d Msg.MaxInstanceID %lu systemvariables_size %d mastervariables_size %d",
    msg.GetInstanceID(), self.GetInstanceId(), msg.GetNodeID(), msg.GetNowInstanceID(),
    len(msg.GetSystemVariables()), len(msg.GetMasterVariables()))

  self.SetSeenInstanceID(msg.GetNowInstanceID(), msg.GetNodeID())

  bSystemVariablesChange := false
  self.config.GetSystemVSM()
}

func (self *Learner) OnComfirmAskForLearn(msg *common.PaxosMsg) {

}
