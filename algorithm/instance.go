package algorithm

import (
  "errors"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
)

type Instance struct {
  config        *config.Config
  transport     common.MsgTransport
  factory       *sm_base.StateMachineFactory
  iothread      *IOThread
  acceptor      *Acceptor
  learner       *Learner
  proposer      *Proposer
  paxosLog      *logstorage.PaxosLog
  lastCksum     uint32
  commitCtx     *CommitContext
  commitTimerId uint32
  commiter      *Committer
  cpMng         *checkpoint.CheckpointManager
  timeStat      *util.TimeStat
}

func NewInstance(config *config.Config, logStorage logstorage.LogStorage,
  transport common.MsgTransport, useCheckpointReplayer bool) *Instance {
  instance := &Instance{
    factory:       sm_base.NewStateMachineFactory(config.GetMyGroupIdx()),
    commitCtx:     NewCommitContext(config),
    paxosLog:      logstorage.NewPaxosLog(logStorage),
    config:        config,
    transport:     transport,
    commitTimerId: 0,
    lastCksum:     0,
    timeStat:      util.NewTimeStat(),
  }

  instance.iothread = NewIOThread(config, instance)
  instance.acceptor = NewAcceptor(config, transport, instance, logStorage)
  instance.learner = NewLearner(config, transport, instance, instance.acceptor,
    logStorage, instance.iothread, instance.cpMng, instance.factory)
  instance.proposer = NewProposer(config, transport, instance, instance.learner, instance.iothread)
  instance.commiter = NewCommitter(config, instance.commitCtx, instance.iothread, instance.factory)
  instance.cpMng = checkpoint.NewCheckpointManager(config, instance.factory, logStorage, useCheckpointReplayer)

  return instance
}

func (self *Instance) Stop() {
  self.iothread.Stop()
  self.cpMng.Stop()
  self.learner.Stop()
}

func (self *Instance) Init() error {
  self.learner.Init()

  err := self.acceptor.Init()
  if err != nil {
    log.Error("Acceptor.Init fail %v", err)
    return err
  }

  err = self.cpMng.Init()
  if err != nil {
    log.Error("CheckpointMgr.Init fail %v", err)
    return err
  }

  cpInstanceId := self.cpMng.GetCheckpointInstanceID() + 1
  log.Info("Acceptor.OK, Log.InstanceID %d Checkpoint.InstanceID %d",
    self.acceptor.GetInstanceId(), cpInstanceId)

  nowInstanceId := cpInstanceId
  acceptorInstanceId := self.acceptor.GetInstanceId()
  if nowInstanceId < acceptorInstanceId {
    err := self.PlayLog(nowInstanceId, acceptorInstanceId)
    if err != nil {
      return err
    }

    log.Info("PlayLog OK, begin instanceid %d end instanceid %d", nowInstanceId, acceptorInstanceId)
    nowInstanceId = acceptorInstanceId
  } else {
    if nowInstanceId > acceptorInstanceId {
      self.acceptor.InitForNewPaxosInstance()
    }

    self.acceptor.SetInstanceId(nowInstanceId)
  }

  log.Info("NowInstanceID %d", nowInstanceId)

  self.learner.SetInstanceId(nowInstanceId)
  self.proposer.SetInstanceId(nowInstanceId)
  self.proposer.SetStartProposalID(self.acceptor.GetAcceptorState().GetPromiseNum().proposalId + 1)

  self.cpMng.SetMaxChosenInstanceID(nowInstanceId)

  err = self.InitLastCheckSum()
  if err!= nil {
    return err
  }

  self.learner.Reset_AskforLearn_Noop(common.GetAskforLearnInterval())

  self.iothread.Start()
  self.cpMng.Start()

  log.Info("OK")
  return nil
}

func (self *Instance) InitLastCheckSum() error {
  if self.acceptor.GetInstanceId() == 0 {
    self.lastCksum = 0
    return nil
  }

  if self.acceptor.GetInstanceId() <= self.cpMng.GetMinChosenInstanceID() {
    self.lastCksum = 0
    return nil
  }

  var state common.AcceptorStateData
  err := self.paxosLog.ReadState(self.config.GetMyGroupIdx(), self.acceptor.GetInstanceId()-1, &state)
  if err != nil && err != common.ErrKeyNotFound {
    return err
  }

  if err == common.ErrKeyNotFound {
    self.lastCksum = 0
    return nil
  }

  self.lastCksum = state.GetChecksum()

  log.Info("ok, lastchsum: %d", self.lastCksum)

  return nil
}

func (self *Instance) GetLastChecksum() uint32 {
  return self.lastCksum
}

func (self *Instance) GetCommitter() *Committer {
  return self.commiter
}

func (self *Instance) GetCheckpointCleaner() *checkpoint.Cleaner {
  return self.cpMng.GetCleaner()
}

func (self *Instance) GetCheckpointReplayer() *checkpoint.Replayer {
  return self.cpMng.GetReplayer()
}

func (self *Instance) CheckNewValue() {
  commitCtx := self.commitCtx

  if !commitCtx.IsNewCommit() {
    return
  }

  if !self.learner.IsIMLatest() {
    return
  }

  if self.config.IsIMFollower() {
    commitCtx.SetResultOnlyRet(gpaxos.PaxosTryCommitRet_Follower_Cannot_Commit)
    return
  }

  if !self.config.CheckConfig() {
    log.Error("I'm not in membership, skip this new value")
    commitCtx.SetResultOnlyRet(gpaxos.PaxosTryCommitRet_Im_Not_In_Membership)
    return
  }

  if len(commitCtx.GetCommitValue()) > common.GetMaxValueSize() {
    log.Error("value size %d to large, skip this new value", len(self.commitCtx.GetCommitValue()))
    commitCtx.SetResultOnlyRet(gpaxos.PaxosTryCommitRet_Value_Size_TooLarge)
    return
  }

  commitCtx.StartCommit(self.proposer.GetInstanceId())

  if commitCtx.GetTimeoutMs() != -1 {
    self.commitTimerId = self.iothread.AddTimer(commitCtx.GetTimeoutMs(), common.Timer_Instance_Commit_Timeout)
  }

  self.timeStat.Point()

  if self.config.GetIsUseMembership() &&
    (self.proposer.GetInstanceId() == 0 || self.config.GetGid() == 0) {
    // TODO
    log.Info("Need to init system variables, Now.InstanceID %d Now.Gid %d",
      self.proposer.GetInstanceId(), self.config.GetGid())

  } else {
    self.proposer.NewValue(commitCtx.GetCommitValue())
  }
}

func (self *Instance) OnNewValueCommitTimeout() {
  self.proposer.exitPrepare()
  self.proposer.exitAccept()

  self.commitCtx.SetResult(gpaxos.PaxosTryCommitRet_Timeout, self.proposer.GetInstanceId(), []byte(""))
}

func (self *Instance) OnReceiveMsg(msg string) error {
  self.iothread.AddMessage(msg)
  return nil
}

func (self *Instance) ReceiveMsgHeaderCheck(header *common.Header, fromNodeId uint64) bool {
  if self.config.GetGid() == 0 || header.GetGid() == 0 {
    return true
  }

  if self.config.GetGid() != header.GetGid() {
    return false
  }

  return true
}

func (self *Instance) OnReceive(buffer []byte) {
  if len(buffer) <= 6 {
    log.Error("buffer size too short")
    return
  }

  var header common.Header
  var bodyStartPos int32 = 0
  var bodyLen int32 = 0
  err := UnpackBaseMsg(buffer, &header, &bodyStartPos, &bodyLen)
  if err != nil {
    return
  }

  cmd := header.GetCmdid()
  if cmd == common.MsgCmd_PaxosMsg {
    if self.cpMng.InAskforcheckpointMode() {
      log.Info("in ask for checkpoint mode, ignord paxosmsg")
      return
    }

    var paxosMsg common.PaxosMsg
    err = proto.Unmarshal(buffer[bodyStartPos:bodyStartPos+bodyLen], &paxosMsg)
    if err != nil {
      log.Error("PaxosMsg.ParseFromArray fail:%v", err)
      return
    }

    if !self.ReceiveMsgHeaderCheck(&header, paxosMsg.GetNodeID()) {
      return
    }

    self.OnReceivePaxosMsg(&paxosMsg, false)
  } else if cmd == common.MsgCmd_CheckpointMsg {
    var msg common.CheckpointMsg
    err = proto.Unmarshal(buffer[bodyStartPos:bodyStartPos+bodyLen], &msg)
    if err != nil {
      log.Error("CheckpointMsg.ParseFromArray fail:%v", err)
      return
    }

    if !self.ReceiveMsgHeaderCheck(&header, msg.GetNodeID()) {
      return
    }

    self.OnReceiveCheckpointMsg(&msg)
  }
}

func (self *Instance) OnReceivePaxosMsg(msg *common.PaxosMsg, isRetry bool) error {
  msgType := msg.GetMsgType()

  if msgType == common.MsgType_PaxosPrepareReply ||
    msgType == common.MsgType_PaxosAcceptReply ||
    msgType == common.MsgType_PaxosProposal_SendNewValue {
    if !self.config.IsValidNodeID(msg.GetNodeID()) {
      log.Error("acceptor reply type msg, from nodeid not in my membership, skip this message")
      return nil
    }

    return self.ReceiveMsgForProposer(msg)
  }

  if msgType == common.MsgType_PaxosPrepare ||
    msgType == common.MsgType_PaxosAccept {
    if self.config.GetGid() == 0 {
      self.config.AddTmpNodeOnlyForLearn(msg.GetNodeID())
    }

    if !self.config.IsValidNodeID(msg.GetNodeID()) {
      self.config.AddTmpNodeOnlyForLearn(msg.GetNodeID())
      return nil
    }

    self.ChecksumLogic(msg)
    return self.ReceiveMsgForAcceptor(msg, isRetry)
  }

  if msgType == common.MsgType_PaxosLearner_AskforLearn ||
    msgType == common.MsgType_PaxosLearner_SendLearnValue ||
    msgType == common.MsgType_PaxosLearner_ProposerSendSuccess ||
    msgType == common.MsgType_PaxosLearner_ComfirmAskforLearn ||
    msgType == common.MsgType_PaxosLearner_SendNowInstanceID ||
    msgType == common.MsgType_PaxosLearner_SendLearnValue_Ack ||
    msgType == common.MsgType_PaxosLearner_AskforCheckpoint {
    self.ChecksumLogic(msg)
    return self.ReceiveMsgForLearner(msg)
  }

  log.Error("invalid msgtype %d", msgType)
  return nil
}

func (self *Instance) ReceiveMsgForProposer(msg *common.PaxosMsg) error {
  if self.config.IsIMFollower() {
    log.Error("I'm follower, skip this message")
    return nil
  }

  if msg.GetInstanceID() != self.proposer.GetInstanceId() {
    return nil
  }

  msgType := msg.GetMsgType()
  if msgType == common.MsgType_PaxosPrepareReply {
    self.proposer.OnPrepareReply(msg)
  } else if msgType == common.MsgType_PaxosAcceptReply {
    self.proposer.OnAcceptReply(msg)
  }

  return nil
}

func (self *Instance) ReceiveMsgForAcceptor(msg *common.PaxosMsg, isRetry bool) error {
  if self.config.IsIMFollower() {
    log.Error("I'm follower, skip this message")
    return nil
  }

  if msg.GetInstanceID() != self.acceptor.GetInstanceId() {
  }

  if msg.GetInstanceID() == self.acceptor.GetInstanceId() + 1 {
    newMsg := *msg
    newMsg.InstanceID = proto.Uint64(self.acceptor.GetInstanceId())
    newMsg.MsgType = proto.Int32(common.MsgType_PaxosLearner_ProposerSendSuccess)
    self.ReceiveMsgForLearner(&newMsg)
  }

  if msg.GetInstanceID() == self.acceptor.GetInstanceId() {
    if msg.GetMsgType() == common.MsgType_PaxosPrepare {
      return self.acceptor.OnPrepare(msg)
    } else if msg.GetMsgType() == common.MsgType_PaxosAccept {
      self.acceptor.OnAccept(msg)
    }
  } else if !isRetry && msg.GetInstanceID() > self.acceptor.GetInstanceId() {
    if msg.GetInstanceID() >= self.learner.GetSeenLatestInstanceID() {
      if msg.GetInstanceID() < self.acceptor.GetInstanceId()+RETRY_QUEUE_MAX_LEN {
        self.iothread.AddRetryPaxosMsg(msg)
      } else {
        self.iothread.ClearRetryQueue()
      }
    }
  }

  return nil
}

func (self *Instance) ReceiveMsgForLearner(msg *common.PaxosMsg) error {
  msgType := msg.GetMsgType()

  switch msgType {
  case common.MsgType_PaxosLearner_AskforLearn:
    self.learner.OnAskforLearn(msg)
    break
  case common.MsgType_PaxosLearner_SendLearnValue:
    self.learner.OnSendLearnValue(msg)
    break
  case common.MsgType_PaxosLearner_ProposerSendSuccess:
    self.learner.OnProposerSendSuccess(msg)
    break
  case common.MsgType_PaxosLearner_SendNowInstanceID:
    self.learner.OnSendNowInstanceID(msg)
    break
  case common.MsgType_PaxosLearner_ComfirmAskforLearn:
    self.learner.OnComfirmAskForLearn(msg)
    break
  case common.MsgType_PaxosLearner_SendLearnValue_Ack:
    self.learner.OnSendLearnValue_Ack(msg)
    break
  case common.MsgType_PaxosLearner_AskforCheckpoint:
    self.learner.OnAskforCheckpoint(msg)
    break
  }

  learner := self.learner
  if learner.IsLearned() {
    isMycommit, ctx := self.commitCtx.IsMyCommit(learner.GetInstanceId(), learner.GetLearnValue())
    if isMycommit {
      log.Debug("this value is not my commit")
    } else {
      useTimeMs := self.timeStat.Point()
      log.Info("My commit ok, usetime %d ms", useTimeMs)
    }

    if !self.Execute(learner.GetInstanceId(), learner.GetLearnValue(), isMycommit, ctx) {
      log.Error("SMExecute fail, instanceid %d, not increase instanceid", learner.GetInstanceId())
      self.commitCtx.SetResult(gpaxos.PaxosTryCommitRet_ExecuteFail, learner.GetInstanceId(), learner.GetLearnValue())
      self.proposer.CancelSkipPrepare()
      return errors.New("execute fail")
    }

    {
      self.commitCtx.SetResult(gpaxos.PaxosTryCommitRet_OK, learner.GetInstanceId(), learner.GetLearnValue())
      if self.commitTimerId > 0 {
        self.iothread.RemoveTimer(&self.commitTimerId)
      }
    }

    log.Info("[Learned] New paxos starting, Now.Proposer.InstanceID %d" +
      "Now.Acceptor.InstanceID %d Now.Learner.InstanceID %d",
      self.proposer.GetInstanceId(), self.acceptor.GetInstanceId(), learner.GetInstanceId())

    log.Info("[Learned] Checksum change, last checksum %d new checksum %d",
      self.lastCksum, learner.GetNewChecksum())

    self.lastCksum = learner.GetLastChecksum()

    self.NewInstance()

    log.Info("[Learned] New paxos instance has started, Now.Proposer.InstanceID %d" +
      "Now.Acceptor.InstanceID %d Now.Learner.InstanceID %d",
      self.proposer.GetInstanceId(), self.acceptor.GetInstanceId(), learner.GetInstanceId())

    self.cpMng.SetMaxChosenInstanceID(self.acceptor.GetInstanceId())
  }

  return nil
}

func (self *Instance) OnReceiveCheckpointMsg(msg *common.CheckpointMsg) {
  log.Info("Now.InstanceID %d MsgType %d Msg.from_nodeid %d My.nodeid %d flag %d"+
    " uuid %d sequence %d checksum %d offset %d buffsize %d filepath %s",
    self.acceptor.GetInstanceId(), msg.GetMsgType(), msg.GetNodeID(),
    self.config.GetMyNodeId(), msg.GetFlag(), msg.GetUUID(), msg.GetSequence(),
    msg.GetChecksum(), msg.GetOffset(), len(msg.GetBuffer()), msg.GetFilePath())

  if msg.GetMsgType() == common.CheckpointMsgType_SendFile {
    if !self.cpMng.InAskforcheckpointMode() {
      log.Info("not in ask for checkpoint mode, ignord checkpoint msg")
      return
    }
  } else if msg.GetMsgType() == common.CheckpointMsgType_SendFile_Ack {
    self.learner.OnSendCheckpointAck(msg)
  }
}

func (self *Instance) NewInstance() {
  self.acceptor.NewInstance()
  self.learner.NewInstance()
  self.proposer.NewInstance()
}

func (self *Instance) GetNowInstanceID() uint64 {
  return self.acceptor.GetInstanceId()
}

func (self *Instance) OnTimeout(timerId uint32, timerType int) {
  switch timerType {
  case common.Timer_Proposer_Prepare_Timeout:
    self.proposer.OnPrepareTimeout()
    break
  case common.Timer_Proposer_Accept_Timeout:
    self.proposer.OnAcceptTimeout()
    break
  case common.Timer_Learner_Askforlearn_noop:
    self.learner.AskforLearn_Noop(false)
    break
  case common.Timer_Instance_Commit_Timeout:
    self.OnNewValueCommitTimeout()
    break
  default:
    log.Error("unknown timer type %d, timerid %d", timerType, timerId)
    break
  }
}

func (self *Instance)AddStateMachine(statemachine gpaxos.StateMachine)  {
  self.factory.AddStateMachine(statemachine)
}

func (self *Instance) Execute(instanceId uint64, value []byte, isMyCommit bool,
                              ctx *gpaxos.StateMachineContext) bool {
  return self.factory.Execute(self.config.GetMyGroupIdx(), instanceId, value, ctx)
}

func (self *Instance) ChecksumLogic(msg *common.PaxosMsg) {
  if msg.GetLastChecksum() == 0 {
    return
  }

  if msg.GetInstanceID() != self.acceptor.GetInstanceId() {
    return
  }

  if self.acceptor.GetInstanceId() > 0 && self.GetLastChecksum() == 0 {
    log.Error("I have no last checksum, other last checksum %d", msg.GetLastChecksum())
    self.lastCksum = msg.GetLastChecksum()
    return
  }

  if msg.GetLastChecksum() != self.GetLastChecksum() {
    log.Error("checksum fail, my last checksum %d other last checksum %d",
      self.GetLastChecksum(), msg.GetLastChecksum())
  }
}

func (self *Instance) PlayLog(begin uint64, end uint64) error {
  if begin < self.cpMng.GetMinChosenInstanceID() {
    log.Error("now instanceid %d small than min chosen instanceid %d",
      begin, self.cpMng.GetMinChosenInstanceID())
    return errors.New("invalid instanceid")
  }
  groupIdx := self.config.GetMyGroupIdx()

  for instanceId := begin; instanceId < end; instanceId++ {
    var state common.AcceptorStateData
    err := self.paxosLog.ReadState(groupIdx, instanceId, &state)
    if err != nil {
      log.Error("log read fail %v instanceid %d", err, instanceId)
      return err
    }

    ok := self.factory.Execute(groupIdx, instanceId, state.GetAcceptedValue(), nil)
    if !ok {
      log.Error("Execute fail, instanceid %d", instanceId)
      return errors.New("execute fail")
    }
  }

  return nil
}

func (self *Instance)GetInstanceValue(instanceId uint64, value []byte, smid *int32) error {
  *smid = 0
  if instanceId >= self.acceptor.GetInstanceId() {
    return gpaxos.Paxos_GetInstanceValue_Value_Not_Chosen_Yet
  }

  var state common.AcceptorStateData
  err := self.paxosLog.ReadState(self.config.GetMyGroupIdx(), instanceId, &state)
  if err != nil && err != common.ErrKeyNotFound {
    return err
  }

  if err == common.ErrKeyNotFound {
    return gpaxos.Paxos_GetInstanceValue_Value_NotExist
  }

  util.DecodeInt32(state.GetAcceptedValue(), 0, smid)
  value = util.CopyBytes(state.GetAcceptedValue()[util.INT32SIZE:])

  return nil
}