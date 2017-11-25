package algorithm

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/storage"
  "github.com/lichuang/gpaxos/network"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
)

type Instance struct {
  config *config.Config
  logStorage *storage.LogStorage
  paxosLog     *storage.PaxosLog
  committer *Committer
  commitctx *CommitContext
  proposer *Proposer
  learner *Learner
  acceptor *Acceptor
  name string

  transport network.Transport

  timerThread *util.TimerThread

  end chan bool

  commitChan chan CommitMsg
}

func NewInstance(config *config.Config, logStorage *storage.LogStorage) *Instance {
  instance := &Instance{
    config:      config,
    logStorage:  logStorage,
    paxosLog:    storage.NewPaxosLog(logStorage),
    timerThread: util.NewTimerThread(),
    end:         make(chan bool),
    commitChan:  make(chan CommitMsg),
  }
  instance.initNetwork(config.GetOptions())

  instance.commitctx = newCommitContext(instance)
  instance.committer = newCommitter(instance)
  // learner must create before proposer
  instance.acceptor = NewAcceptor(instance)
  instance.learner = NewLearner(instance)
  instance.proposer = NewProposer(instance)

  instance.name = config.GetOptions().MyNode.String()
  start := make(chan bool)
  go instance.main(start)
  <- start

  return instance
}

func (self *Instance)initNetwork(options *gpaxos.Options) *Instance {
  self.transport = network.NewNetwork(options, NewPaxosSessionFactory(self))
  return self
}

func (self *Instance) main(start chan bool) {
  start <- true

  end := false
  for !end {
    select {
    case <-self.end:
      end = true
      break
    case <-self.commitChan:
      self.onCommit()
      break
    }
  }
}

func (self *Instance) Status(instanceId uint64) Status {
  if instanceId <= self.acceptor.GetInstanceId() {
    return Decided
  }

  return Pending
}

// try to propose a value, return instanceid end error
func (self *Instance) Propose(value []byte) (uint64, error) {
  log.Debug("[%s]try to propose value %s", self.name, string(value))
  return self.committer.NewValue(value)
}

func (self *Instance) sendCommitMsg() {
  self.commitChan <- CommitMsg{}
  log.Debug("send commit msg")
}

// handle commit message
func (self *Instance) onCommit() {
  log.Debug("[%s]on commit", self.name)
  if !self.commitctx.isNewCommit() {
    return
  }

  if !self.learner.IsImLatest() {
    return
  }

  if self.config.IsIMFollower() {
    log.Error("[%s]I'm follower, skip commit new value", self.name)
    self.commitctx.setResultOnlyRet(gpaxos.PaxosTryCommitRet_Follower_Cannot_Commit)
    return
  }

  commitValue := self.commitctx.getCommitValue()
  if len(commitValue) > common.GetMaxValueSize() {
    log.Error("[%s]value size %d to large, skip commit new value", self.name, len(commitValue))
    self.commitctx.setResultOnlyRet(gpaxos.PaxosTryCommitRet_Value_Size_TooLarge)
  }

  // ok, now do commit
  self.commitctx.startCommit(self.proposer.GetInstanceId())

  self.proposer.newValue(self.commitctx.getCommitValue())
}

func (self *Instance) String() string {
  return self.name
}

func (self *Instance) GetLastChecksum() uint32 {
  return 0
}

func (self *Instance) isCheckSumValid(msg *common.PaxosMsg) bool {
  return true
}

func (self *Instance) NewInstance() {
  self.acceptor.NewInstance()
  self.proposer.NewInstance()
  self.learner.NewInstance()
}

func (self *Instance) receiveMsgForLearner(msg *common.PaxosMsg) error {
  log.Info("[%s]recv msg %d for learner", self.name, msg.GetMsgType())
  learner := self.learner
  msgType := msg.GetMsgType()

  if msgType == common.MsgType_PaxosLearner_ProposerSendSuccess {
    learner.OnProposerSendSuccess(msg)
  }
  if learner.IsLearned() {
    // this paxos instance end
    self.commitctx.setResult(gpaxos.PaxosTryCommitRet_OK, learner.GetInstanceId(), learner.GetLearnValue())

    self.NewInstance()

    log.Info("[%s]new paxos instance has started, Now instance id:proposer %d, acceptor %d, learner %d",
      self.name, self.proposer.GetInstanceId(), self.acceptor.GetInstanceId(), self.learner.GetInstanceId())
  }
  return nil
}

func (self *Instance) receiveMsgForProposer(msg *common.PaxosMsg) error {
  log.Debug("[%s]receive proposer %d",self.name,msg.GetMsgType())
  if self.config.IsIMFollower() {
    log.Error("[%s]follower skip %d msg", self.name, msg.GetMsgType())
    return nil
  }

  msgInstanceId := msg.GetInstanceID()
  proposerInstanceId := self.proposer.GetInstanceId()

  if msgInstanceId != proposerInstanceId {
    return nil
  }

  msgType := msg.GetMsgType()
  if msgType == common.MsgType_PaxosPrepareReply {
    return self.proposer.OnPrepareReply(msg)
  } else if msgType == common.MsgType_PaxosAcceptReply {
    return self.proposer.OnAcceptReply(msg)
  }

  return common.ErrInvalidMsg
}

// handle msg type which for acceptor
func (self *Instance) receiveMsgForAcceptor(msg *common.PaxosMsg, isRetry bool) error {
  if self.config.IsIMFollower() {
    log.Error("[%s]follower skip %d msg", self.name, msg.GetMsgType())
    return nil
  }

  msgInstanceId := msg.GetInstanceID()
  acceptorInstanceId := self.acceptor.GetInstanceId()

  // msgInstanceId == acceptorInstanceId + 1  means this instance has been approved
  // so just learn it
  if msgInstanceId == acceptorInstanceId + 1 {
    newMsg := &common.PaxosMsg{ }
    util.CopyStruct(newMsg, *msg)
    newMsg.InstanceID = proto.Uint64(acceptorInstanceId)
    newMsg.MsgType = proto.Int(common.MsgType_PaxosLearner_ProposerSendSuccess)
    log.Debug("learn it, node id: %d:%d", newMsg.GetNodeID(), msg.GetNodeID())
    self.receiveMsgForLearner(newMsg)
  }

  msgType := msg.GetMsgType()

  // msg instance == acceptorInstanceId means this msg is what acceptor processing
  // so call the acceptor function to handle it
  if msgInstanceId == acceptorInstanceId {
    if msgType == common.MsgType_PaxosPrepare {
      return self.acceptor.onPrepare(msg)
    } else if msgType == common.MsgType_PaxosAccept {
      return self.acceptor.onAccept(msg)
    }

    // never reach here
    log.Error("wrong msg type %d", msgType)
    return common.ErrInvalidMsg
  }

  // ignore retry msg
  if isRetry {
    log.Info("ignore retry msg")
    return nil
  }

  // ignore processed msg
  if msgInstanceId <= acceptorInstanceId {
    return nil
  }

  if msgInstanceId >= self.learner.getSeenLatestInstanceId() {

  }
  return nil
}

func (self *Instance) OnReceivePaxosMsg(msg *common.PaxosMsg, isRetry bool) error {
  proposer := self.proposer
  learner := self.learner
  msgType := msg.GetMsgType()

  log.Info("[%s]instance id %d, msg instance id:%d, msgtype: %d, from: %d, my node id:%d, latest instanceid %d",
    self.name, proposer.GetInstanceId(), msg.GetInstanceID(), msgType, msg.GetNodeID(),
    self.config.GetMyNodeId(),learner.getSeenLatestInstanceId())

  // handle msg for acceptor
  if msgType == common.MsgType_PaxosPrepare || msgType == common.MsgType_PaxosAccept {
    if !self.config.IsValidNodeID(msg.GetNodeID()) {
      self.config.AddTmpNodeOnlyForLearn(msg.GetNodeID())
      return nil
    }

    if !self.isCheckSumValid(msg) {
      return common.ErrInvalidMsg
    }

    return self.receiveMsgForAcceptor(msg, isRetry)
  }

  // handle paxos prepare and accept reply msg
  if (msgType == common.MsgType_PaxosPrepareReply || msgType == common.MsgType_PaxosAcceptReply) {
    return self.receiveMsgForProposer(msg)
  }

  // handler msg for learner
  if (msgType == common.MsgType_PaxosLearner_AskforLearn ||
      msgType == common.MsgType_PaxosLearner_SendLearnValue ||
      msgType == common.MsgType_PaxosLearner_ProposerSendSuccess ||
      msgType == common.MsgType_PaxosLearner_ConfirmAskforLearn ||
      msgType == common.MsgType_PaxosLearner_SendNowInstanceID ||
      msgType == common.MsgType_PaxosLearner_SendLearnValue_Ack ||
      msgType == common.MsgType_PaxosLearner_AskforCheckpoint) {
    if !self.isCheckSumValid(msg) {
      return common.ErrInvalidMsg
    }

    return self.receiveMsgForLearner(msg)
  }

  log.Error("invalid msg %d", msgType)
  return common.ErrInvalidMsg
}

func (self *Instance)OnReceiveMsg(buffer []byte, cmd int32) error {
  log.Debug("[%s]recv %d msg", self.name, cmd)
  if cmd == common.MsgCmd_PaxosMsg {
    var msg common.PaxosMsg
    err := proto.Unmarshal(buffer, &msg)
    if err != nil {
      return err
    }
    return self.OnReceivePaxosMsg(&msg, false)
  }

  return nil
}
