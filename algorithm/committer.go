package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/statemachine"
	"github.com/lichuang/gpaxos/util"
	"fmt"
)

const (
  MaxTryCount = 3
)

type Committer struct {
  config    *config.Config
  commitCtx *CommitContext
  factory   *statemachine.StatemachineFactory

  instance *Instance

  timeoutMs   uint64
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
  return self.newValueGetID(value, nil)
}

func (self *Committer) newValueGetID(value []byte, context *gpaxos.StateMachineContext) (uint64, error) {
  err := gpaxos.PaxosTryCommitRet_OK
  var instanceid uint64
  for retryCnt := MaxTryCount; retryCnt > 0; retryCnt-- {
    instanceid, err = self.newValueGetIDNoRetry(value, context)

    if err != gpaxos.PaxosTryCommitRet_Conflict {
      break
    }

    if context != nil && context.SMId == gpaxos.MASTER_V_SMID {
      break
    }
  }

  return instanceid, err
}

func (self *Committer) newValueGetIDNoRetry(value []byte, context *gpaxos.StateMachineContext) (uint64, error) {
	start := util.NowTimeMs()
	_, err := self.waitLock.Lock(5000)
	if err == util.Waitlock_Timeout {
		fmt.Printf("diff: %d\n", util.NowTimeMs() - start)
		return 0, gpaxos.PaxosTryCommitRet_Timeout
	}

  var smid int32 = 0
  if context != nil {
    smid = context.SMId
  }

  packValue := self.factory.PackPaxosValue(value, smid)
  self.commitCtx.newCommit(packValue, context)
  self.instance.sendCommitMsg()

  instanceId, err := self.commitCtx.getResult()

	self.waitLock.Unlock()

	return instanceId, err
}