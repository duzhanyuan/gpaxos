package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/statemachine"
  "sync"
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

  mutex sync.Mutex
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
  self.mutex.Lock()
  defer self.mutex.Unlock()

  var smid int32 = 0
  if context != nil {
    smid = context.SMId
  }

  packValue := self.factory.PackPaxosValue(value, smid)
  self.commitCtx.newCommit(packValue, context)
  self.instance.sendCommitMsg()

  return self.commitCtx.getResult()
}