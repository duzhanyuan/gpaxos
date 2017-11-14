package algorithm

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos"
  "bytes"
  "sync"
  "github.com/lichuang/gpaxos/util"
)

type CommitContext struct {
  instanceId          uint64
  commitEnd           bool
  value               []byte
  stateMachineContext *gpaxos.StateMachineContext
  mutex               sync.Mutex
  commitRet           error

  // the start and end time of commit(in ms)
  start               uint64
  end                 uint64

  instance            *Instance

  // wait result channel
  wait                chan bool
}

func newCommitContext(instance *Instance) *CommitContext {
  context := &CommitContext{
    value:    nil,
    instance: instance,
    wait:     make(chan bool),
  }
  //context.newCommit(nil, nil)

  return context
}

func (self *CommitContext) newCommit(value []byte, context *gpaxos.StateMachineContext) {
  self.mutex.Lock()

  self.instanceId = common.INVALID_INSTANCEID
  self.commitEnd = false
  self.value = value
  self.stateMachineContext = context
  self.end = 0
  self.start = util.NowTimeMs()

  self.mutex.Unlock()

  self.instance.sendCommitMsg()
}

func (self *CommitContext) isNewCommit() bool {
  return self.instanceId == common.INVALID_INSTANCEID && self.value != nil
}

func (self *CommitContext) startCommit(instanceId uint64) {
  self.mutex.Lock()
  self.instanceId = instanceId
  self.mutex.Unlock()
}

func (self *CommitContext) getCommitValue() [] byte {
  return self.value
}

func (self *CommitContext) isMyCommit(instanceId uint64, learnValue []byte)(bool,*gpaxos.StateMachineContext) {
  self.mutex.Lock()

  var ctx *gpaxos.StateMachineContext
  isMyCommit := false

  if !self.commitEnd && self.instanceId == instanceId {
    if bytes.Compare(self.value, learnValue) == 0 {
      isMyCommit = true
    } else {
      isMyCommit = false
    }
  }

  if isMyCommit {
    ctx = self.stateMachineContext
  }
  self.mutex.Unlock()

  return isMyCommit, ctx
}

func (self *CommitContext) setResultOnlyRet(commitret error) {
  self.setResult(commitret, common.INVALID_INSTANCEID, []byte(""))
}

func (self *CommitContext) setResult(commitret error, instanceId uint64, learnValue []byte) {
  self.mutex.Lock()

  if self.commitEnd || self.instanceId != instanceId {
    self.mutex.Unlock()
    return
  }

  self.commitRet = commitret
  if self.commitRet == nil {
    if bytes.Compare(self.value, learnValue) != 0 {
      self.commitRet = gpaxos.PaxosTryCommitRet_Conflict
    }
  }

  self.commitEnd = true
  self.value = nil

  self.mutex.Unlock()

  self.wait <- true
}

func (self *CommitContext) getResult() (uint64, error) {
  /*
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
  */

  <-self.wait
  self.end = util.NowTimeMs()
  return 0, self.commitRet
}
