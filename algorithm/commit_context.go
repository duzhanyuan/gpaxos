package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos"
  "bytes"
  "sync"
)

type CommitContext struct {
  Config              *config.Config
  InstanceId          uint64
  IsCommitEnd         bool
  TimeoutMs           uint32
  Value               []byte
  StateMachineContext *gpaxos.StateMachineContext
  Mutext              sync.Mutex
  Serialock           *util.TimeoutCond
  CommitRet           error
}

func NewCommitContext(config *config.Config) *CommitContext {
  context := &CommitContext{
    Config: config,
    Value:  nil,
  }
  context.Serialock = util.NewTimeoutCond()
  context.NewCommit(nil, nil, 0)

  return context
}

func (self *CommitContext) NewCommit(value []byte, context *gpaxos.StateMachineContext, timeout uint32) {
  self.Mutext.Lock()

  self.InstanceId = common.INVALID_INSTANCEID
  self.IsCommitEnd = false
  self.TimeoutMs = timeout
  self.Value = value
  self.StateMachineContext = context

  self.Mutext.Unlock()
}

func (self *CommitContext) IsNewCommit() bool {
  if self.InstanceId != common.INVALID_INSTANCEID && self.Value != nil {
    return true
  }

  return false
}

func (self *CommitContext) GetCommitValue() []byte {
  return self.Value
}

func (self *CommitContext) StartCommit(instanceId uint64) {
  self.Mutext.Lock()
  self.InstanceId = instanceId
  self.Mutext.Unlock()
}

func (self *CommitContext) IsMyCommit(instanceId uint64, learnValue []byte)(bool,*gpaxos.StateMachineContext) {
  self.Mutext.Lock()

  var ctx *gpaxos.StateMachineContext
  isMyCommit := false

  if !self.IsCommitEnd && self.InstanceId == instanceId {
    if bytes.Compare(self.Value, learnValue) == 0 {
      isMyCommit = true
    } else {
      isMyCommit = false
    }
  }

  if isMyCommit {
    ctx = self.StateMachineContext
  }
  self.Mutext.Unlock()

  return isMyCommit, ctx
}

func (self *CommitContext) SetResultOnlyRet(commitret error) {
  self.SetResult(commitret, common.INVALID_INSTANCEID, []byte(""))
}

func (self *CommitContext) SetResult(commitret error, instanceId uint64, learnValue []byte) {
  self.Mutext.Lock()

  if self.IsCommitEnd || self.InstanceId != instanceId {
    self.Mutext.Unlock()
    return
  }

  self.CommitRet = commitret
  if self.CommitRet == nil {
    if bytes.Compare(self.Value, learnValue) != 0 {
      self.CommitRet = gpaxos.PaxosTryCommitRet_Conflict
    }
  }

  self.IsCommitEnd = true
  self.Value = nil

  self.Serialock.Signal()
  self.Mutext.Unlock()
}

func (self *CommitContext) GetResult(succInstanceId *uint64) error {
  for !self.IsCommitEnd {
    self.Serialock.WaitFor(1000)
  }

  if self.CommitRet == nil {
    *succInstanceId = self.InstanceId
    log.Info("commit success, instanceid %d", self.InstanceId)
  } else {
    log.Error("commit fail: %v", self.CommitRet)
  }

  self.Serialock.Unlock()

  return self.CommitRet
}

func (self *CommitContext) GetTimeoutMs() uint32 {
  return self.TimeoutMs
}
