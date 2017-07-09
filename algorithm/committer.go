package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/lichuang/gpaxos"
)

type Committer struct {
  config    *config.Config
  commitCtx *CommitContext
  iothread  *IOThread
  factory   *sm_base.StateMachineFactory

  timeoutMs   uint64
  LastLogTime uint64
  waitLock    *util.WaitLock
}

func NewCommitter(config *config.Config, ctx *CommitContext, iothread *IOThread,
  factory *sm_base.StateMachineFactory) *Committer {
  return &Committer{
    config:      config,
    commitCtx:   ctx,
    iothread:    iothread,
    factory:     factory,
    LastLogTime: util.NowTimeMs(),
    waitLock:    util.NewWaitLock(),
  }
}

func (self *Committer) NewValue(value []byte) error {
  var instanceId uint64 = 0
  return self.NewValueGetID(value, &instanceId, nil)
}

func (self *Committer) NewValueGetIDNoContext(value []byte, instanceId *uint64) error {
  return self.NewValueGetID(value, instanceId, nil)
}

func (self *Committer) NewValueGetID(value []byte, instanceId *uint64, context *gpaxos.StateMachineContext) error {
  err := gpaxos.PaxosTryCommitRet_OK
  for retryCnt := 3; retryCnt > 0; retryCnt-- {
    err = self.NewValueGetIDNoRetry(value, instanceId, context)
    if err != gpaxos.PaxosTryCommitRet_Conflict {
      break
    }

    if context != nil && context.SMId == gpaxos.MASTER_V_SMID {
      break
    }
  }

  return err
}

func (self *Committer) NewValueGetIDNoRetry(value []byte, instaneId *uint64, context *gpaxos.StateMachineContext) error {
  var lockUseTime uint64 = 0
  hasLock := self.waitLock.Lock(self.timeoutMs, &lockUseTime)
  if !hasLock {
    if lockUseTime > 0 {
      log.Error("Try get lock, but timeout, lockusetime %d ms", lockUseTime)
      return gpaxos.PaxosTryCommitRet_Timeout
    } else {
      log.Error("Try get lock, but too many thread waiting, reject")
      return gpaxos.PaxosTryCommitRet_Reject
    }
  }

  var leftTimeoutMs uint64 = -1
  if self.timeoutMs > 0 {
    leftTimeoutMs = self.timeoutMs - lockUseTime
    if leftTimeoutMs < 0 {
      leftTimeoutMs = 0
    }

    if leftTimeoutMs < 200 {
      log.Error("Get lock ok, but lockusetime %dms too long, lefttimeout %d ms", lockUseTime, leftTimeoutMs)
      self.waitLock.Unlock()
      return gpaxos.PaxosTryCommitRet_Timeout
    }
  }

  log.Info("GetLock ok, use time %d ms", lockUseTime)

  var smid int32 = 0
  if context != nil {
    smid = context.SMId
  }

  pachSMIDValue := value
  self.factory.PackPaxosValue(pachSMIDValue, smid)
  self.commitCtx.NewCommit(pachSMIDValue, context, leftTimeoutMs)
  self.iothread.AddNotify()

  err := self.commitCtx.GetResult(instaneId)

  self.waitLock.Unlock()
  return err
}

func (self *Committer) SetTimeoutMs(timeoutMs uint64) {
  self.timeoutMs = timeoutMs
}

func (self *Committer) SetMaxHoldThreads(maxHoldThreads int) {
  self.waitLock.SetMaxWaitLogCount(maxHoldThreads)
}

func (self *Committer) SetProposeWaitTimeThresholdMS(waitTimeThresholdMS int) {
  self.waitLock.SetLockWaitTimeThreshold(waitTimeThresholdMS)
}

func (self *Committer) LogStatus() {
  nowTime := util.NowTimeMs()
  if nowTime > self.LastLogTime && nowTime-self.LastLogTime > 1000 {
    self.LastLogTime = nowTime
    log.Info("wait threads %d avg thread wait ms %d reject rate %d",
      self.waitLock.GetNowHoldThreadCount(), self.waitLock.GetNowAvgThreadWaitTime(),
      self.waitLock.GetNowRejectRate())
  }
}
