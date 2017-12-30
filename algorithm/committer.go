package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/statemachine"
	"github.com/lichuang/gpaxos/util"
	"github.com/lichuang/gpaxos/common"
)

const (
  MaxTryCount = 3
)

type Committer struct {
  config    *config.Config
  commitCtx *CommitContext
  factory   *statemachine.StatemachineFactory

  instance *Instance

  timeoutMs   uint32
  lastLogTime uint64

  waitLock util.Waitlock
}

func newCommitter(instance *Instance) *Committer {
  return &Committer{
    config:instance.config,
    commitCtx: instance.commitctx,
    factory:instance.factory,
    instance:instance,
  }
}

func (self *Committer) NewValue(value []byte) (uint64, error) {
	self.timeoutMs = common.GetMaxCommitTimeoutMs()
  return self.newValueGetID(value, nil)
}

func (self *Committer) newValueGetID(value []byte, context *gpaxos.StateMachineContext) (uint64, error) {
  err := gpaxos.PaxosTryCommitRet_OK
  var instanceid uint64
  for i := 0; i < MaxTryCount && self.timeoutMs > 0; i++ {
    instanceid, err = self.newValueGetIDNoRetry(value, context)

    if err != gpaxos.PaxosTryCommitRet_Conflict && err != gpaxos.PaxosTryCommitRet_WaitTimeout {
      break
    }

    if context != nil && context.SMId == gpaxos.MASTER_V_SMID {
      break
    }
  }

  return instanceid, err
}

func (self *Committer) newValueGetIDNoRetry(value []byte, context *gpaxos.StateMachineContext) (uint64, error) {
	lockUseTime, err := self.waitLock.Lock(int(self.timeoutMs))
	if err == util.Waitlock_Timeout {
		return 0, gpaxos.PaxosTryCommitRet_WaitTimeout
	}

	if self.timeoutMs <= uint32(200 + lockUseTime) {
		self.waitLock.Unlock()
		self.timeoutMs = 0
		return 0, gpaxos.PaxosTryCommitRet_Timeout
	}

	leftTimeoutMs := self.timeoutMs - uint32(lockUseTime)

 	var smid int32 = 0
  if context != nil {
    smid = context.SMId
  }

  packValue := self.factory.PackPaxosValue(value, smid)
  self.commitCtx.newCommit(packValue, leftTimeoutMs, context)
  self.instance.sendCommitMsg()

  instanceId, err := self.commitCtx.getResult()

	self.waitLock.Unlock()
	return instanceId, err
}