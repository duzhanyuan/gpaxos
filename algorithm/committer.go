package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos"

  log "github.com/lichuang/log4go"
)

type Committer struct {
  config    *config.Config
  commitCtx *CommitContext
  //factory   *sm_base.StateMachineFactory

  instance *Instance

  timeoutMs   uint64
  lastLogTime uint64
}

func newCommitter(instance *Instance) *Committer {
  return &Committer{
    config:instance.config,
    commitCtx: instance.commitctx,
    instance:instance,
  }
}

func (self *Committer) NewValue(value []byte) (uint64, error) {
  return self.newValueGetID(value, nil)
}

func (self *Committer) newValueGetID(value []byte, context *gpaxos.StateMachineContext) (uint64, error) {
  err := gpaxos.PaxosTryCommitRet_OK
  var instanceid uint64
  for retryCnt := 3; retryCnt > 0; retryCnt-- {
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
  /*
  var smid int32 = 0
  if context != nil {
    smid = context.SMId
  }
  */

  packSMIDValue := value
  //self.factory.PackPaxosValue(packSMIDValue, smid)
  self.commitCtx.newCommit(packSMIDValue, context)

  return self.commitCtx.getResult()
}