package algorithm

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos"
  "bytes"
  "sync"
  "github.com/lichuang/gpaxos/util"
  log "github.com/lichuang/log4go"
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

  timeoutMs 					uint32

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

func (self *CommitContext) newCommit(value []byte, timeoutMs uint32, context *gpaxos.StateMachineContext) {
  self.mutex.Lock()

  self.instanceId = common.INVALID_INSTANCEID
  self.commitEnd = false
  self.value = value
  self.stateMachineContext = context
  self.end = 0
  self.start = util.NowTimeMs()
  self.timeoutMs = timeoutMs

  self.mutex.Unlock()
}

func (self *CommitContext) isNewCommit() bool {
  return self.instanceId == common.INVALID_INSTANCEID && self.value != nil
}

func (self *CommitContext) StartCommit(instanceId uint64) uint32 {
  self.mutex.Lock()
  self.instanceId = instanceId
  self.mutex.Unlock()

  return self.timeoutMs
}

func (self *CommitContext) getCommitValue() [] byte {
  return self.value
}

func (self *CommitContext) IsMyCommit(nodeId uint64, instanceId uint64, learnValue []byte)(bool,*gpaxos.StateMachineContext) {
  self.mutex.Lock()
  defer self.mutex.Unlock()

  if nodeId != self.instance.config.GetMyNodeId() {
  	log.Debug("[%s]%d not my instance id", self.instance.String(), nodeId)
    return false, nil
  }

  var ctx *gpaxos.StateMachineContext
  isMyCommit := false

  if !self.commitEnd && self.instanceId == instanceId {
    if bytes.Compare(self.value, learnValue) == 0 {
      isMyCommit = true
    } else {
			log.Debug("[%s]%d not my value", self.instance.String(), instanceId)
      isMyCommit = false
    }
	}

  if isMyCommit {
    ctx = self.stateMachineContext
  } else {
  	log.Debug("[%s]%d not my commit %v:%d", self.instance.String(), instanceId, self.commitEnd, self.instanceId)
	}

  return isMyCommit, ctx
}

func (self *CommitContext) setResultOnlyRet(commitret error) {
  self.setResult(commitret, common.INVALID_INSTANCEID, []byte(""))
}

func (self *CommitContext) setResult(commitret error, instanceId uint64, learnValue []byte) {
  self.mutex.Lock()
  defer self.mutex.Unlock()

  if self.commitEnd || self.instanceId != instanceId {
    log.Error("[%s]set result error, self instance id %d,msg instance id %d", self.instance.String(), self.instanceId, instanceId)
    return
  }

  self.commitRet = commitret
  if self.commitRet == gpaxos.PaxosTryCommitRet_OK {
    if bytes.Compare(self.value, learnValue) != 0 {
      self.commitRet = gpaxos.PaxosTryCommitRet_Conflict
    }
  }

  self.commitEnd = true
  self.value = nil

  log.Debug("[%s]set commit result instance %d ret %v", self.instance.String(),instanceId,self.commitRet)

  self.wait <- true
}

func (self *CommitContext) getResult()(uint64, error) {
  select {
  case <- self.wait:
    break
  }

  if self.commitRet == gpaxos.PaxosTryCommitRet_OK {
    return self.instanceId, self.commitRet
  }

  return 0, self.commitRet
}
