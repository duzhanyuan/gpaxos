package checkpoint

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/common"
  "time"
  "math/rand"
)

const (
  CAN_DELETE_DELTA     = 1000000
  DELETE_SAVE_INTERVAL = 100
)

type Cleaner struct {
  Config     *config.Config
  Factory    *sm_base.StateMachineFactory
  Logstorage logstorage.LogStorage
  CpMng      *CheckpointManager
  LastSave   uint64
  CanRun     bool
  IsPause    bool
  IsEnd      bool
  IsStart    bool
  HoldCount  uint64
}

func NewCleaner(config *config.Config, factory *sm_base.StateMachineFactory,
  storage logstorage.LogStorage, cpmng *CheckpointManager) *Cleaner {
  return &Cleaner{
    Config:     config,
    Factory:    factory,
    Logstorage: storage,
    CpMng:      cpmng,
    HoldCount:  CAN_DELETE_DELTA,
  }
}

func (self *Cleaner) Start () {
  go self.Main()
}

func (self *Cleaner) Stop() {
  self.IsEnd = true
}

func (self *Cleaner) Pause() {
  self.CanRun = false
}

func (self *Cleaner) Continue() {
  self.IsPause = false
  self.CanRun = true
}

func (self *Cleaner) IsPaused() bool {
  return self.IsPause
}

func (self *Cleaner) Main() {
  self.IsStart = true
  self.Continue()

  deleteOps := common.GetCleanerDeleteQps()
  var sleepMs uint32 = 1
  if deleteOps > 1000 {
    sleepMs = deleteOps/1000 + 1
  }
  var deleteInterval uint32 = 1
  if deleteInterval > 1000 {
    deleteInterval = deleteOps/1000 + 1
  }

  for {
    if self.IsEnd {
      log.Info("checkpoint cleaner end")
      return
    }

    if !self.CanRun {
      self.IsPause = true
      time.Sleep(1000 * time.Microsecond)
      continue
    }

    instanceId := self.CpMng.GetMinChosenInstanceID()
    cpInstanceId := self.Factory.GetCheckpointInstanceID(self.Config.GetMyGroupIdx()) + 1
    maxChosenInstanceId := self.CpMng.GetMaxChosenInstanceID()

    var deleteCnt uint32 = 0
    for instanceId+self.HoldCount < cpInstanceId && instanceId+self.HoldCount < maxChosenInstanceId {
      delSuc := self.DeleteOne(instanceId)
      if delSuc {
        instanceId++
        deleteCnt++
        if deleteCnt > deleteInterval {
          deleteCnt = 0
          time.Sleep(time.Duration(sleepMs) * time.Microsecond)
        } else {
          log.Debug("delete system fail,instanceid %d", instanceId)
          break
        }
      }
    }

    if cpInstanceId == 0 {

    } else {

    }

    time.Sleep(time.Duration(rand.Int()%500+500) * time.Microsecond)
  }
}

func (self *Cleaner) DeleteOne(instanceId uint64) bool {
  options := logstorage.WriteOptions{
    Sync: false,
  }

  err := self.Logstorage.Del(options, self.Config.GetMyGroupIdx(), instanceId)
  if err != nil {
    return false
  }

  if instanceId >= self.LastSave+DELETE_SAVE_INTERVAL {
    err = self.CpMng.SetMinChosenInstanceID(instanceId + 1)
    if err != nil {
      log.Error("SetMinChosenInstanceID fail:%v, now delete instanceid %d", err, instanceId)
      return false
    }

    self.LastSave = instanceId

    log.Info("delete %d instance done, now minchosen instanceid %d", DELETE_SAVE_INTERVAL, instanceId+1)
  }

  return true
}

func (self *Cleaner) FixMinChosenInstanceID(oldMinChosenInstanceID uint64) error {
  cpInstanceId := self.Factory.GetCheckpointInstanceID(self.Config.GetMyGroupIdx()) + 1
  fixMinChosenInstanceId := oldMinChosenInstanceID

  for instanceId := oldMinChosenInstanceID; instanceId < oldMinChosenInstanceID+DELETE_SAVE_INTERVAL; instanceId++ {
    if instanceId >= cpInstanceId {
      break
    }

    var value []byte
    err := self.Logstorage.Get(self.Config.GetMyGroupIdx(), instanceId, value)
    if err != nil {
      break
    }

    fixMinChosenInstanceId = instanceId + 1
  }
  if fixMinChosenInstanceId > oldMinChosenInstanceID {
    err := self.CpMng.SetMinChosenInstanceID(fixMinChosenInstanceId)
    if err != nil {
      return err
    }
  }

  log.Info("ok, old minchosen %d fix minchosen %d", oldMinChosenInstanceID, fixMinChosenInstanceId)
  return nil
}

func (self *Cleaner) SetHoldPaxosLogCount(cnt uint64) {
  if cnt < 300 {
    self.HoldCount = 300
  } else {
    self.HoldCount = cnt
  }
}
