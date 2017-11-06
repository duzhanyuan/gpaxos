package algorithm

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/network"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
)

type Instance struct {
  base *Base

  config *config.Config
  logStorage *logstorage.LogStorage
  committer *Committer
  commitctx *CommitContext
  proposer *Proposer
  learner *Learner
  acceptor *Acceptor

  transport network.Transport

  timerThread *util.TimerThread

  end chan bool

  commitChan chan CommitMsg
}

func NewInstance(config *config.Config, logstorage *logstorage.LogStorage) *Instance {
  instance := &Instance{
    config:config,
    logStorage:logstorage,
    timerThread:util.NewTimerThread(),
    end:make(chan bool),

    commitChan:make(chan CommitMsg),
  }

  instance.base = newBase(instance)
  instance.commitctx = newCommitContext(instance)
  instance.committer = newCommitter(instance)
  instance.proposer = newProposer(instance)
  instance.learner = newLearner(instance)

  start := make(chan bool)
  go instance.main(start)
  <- start

  return instance
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
    break
  }
}

// try to propose a value, return instanceid end error
func (self *Instance) Propose(value []byte) (uint64, error) {
  return self.committer.NewValue(value)
}

func (self *Instance) sendCommitMsg() {
  self.commitChan <- CommitMsg{}
}

// handle commit message
func (self *Instance) onCommit() {
  if !self.commitctx.isNewCommit() {
    return
  }

  if !self.learner.IsImLatest() {
    return
  }

  if self.config.IsIMFollower() {
    log.Error("I'm follower, skip commit new value")
    self.commitctx.setResultOnlyRet(gpaxos.PaxosTryCommitRet_Follower_Cannot_Commit)
    return
  }

  commitValue := self.commitctx.getCommitValue()
  if len(commitValue) > common.GetMaxValueSize() {
    log.Error("value size %d to large, skip commit new value", len(commitValue))
    self.commitctx.setResultOnlyRet(gpaxos.PaxosTryCommitRet_Value_Size_TooLarge)
  }

  // ok, now do commit
  self.commitctx.startCommit(self.base.GetInstanceId())

  self.proposer.newValue(self.commitctx.getCommitValue())
}

func (self *Instance) GetLastChecksum() uint32 {
  return 0
}

func (self *Instance) isCheckSumValid(msg *common.PaxosMsg) bool {
  return true
}

func (self *Instance) receiveMsgForLearner(msg *common.PaxosMsg) error {
  return nil
}

func (self *Instance) receiveMsgForProposer(msg *common.PaxosMsg) error {
  if self.config.IsIMFollower() {
    log.Error("follower skip %d msg", msg.GetMsgType())
    return nil
  }

  msgInstanceId := msg.GetInstanceID()
  proposerInstanceId := self.proposer.getInstanceId()

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
    log.Error("follower skip %d msg", msg.GetMsgType())
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

  log.Info("instance id %d, msg instance id:%d, msgtype: %d, from: %d, my node id:%d, latest instanceid %d",
    proposer.getInstanceId(), msg.GetInstanceID(), msgType, msg.GetNodeID(),
    self.config.GetMyNodeId(),learner.getSeenLatestInstanceId())

  // handle paxos prepare and accept msg
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

  return nil
}
