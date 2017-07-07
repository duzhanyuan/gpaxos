package checkpoint


import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/common"
  "time"
)

type Replayer struct {
  IsPause bool
  IsEnd bool
  CanRun bool

  Config *config.Config
  Factory *sm_base.StateMachineFactory
  PaxosLog *logstorage.PaxosLog
  CpMng *CheckpointManager
}

func NewReplayer(config *config.Config, factory *sm_base.StateMachineFactory,
                 storage logstorage.LogStorage, mng *CheckpointManager) *Replayer {
  return &Replayer{
    Config: config,
    Factory:factory,
    PaxosLog: logstorage.NewPaxosLog(storage),
    CpMng:mng,
  }
}

func (self *Replayer) Start() {
  go self.Main()
}

func (self *Replayer) Stop() {
  self.IsEnd = true
}

func (self *Replayer) Continue() {
  self.IsPause = false
  self.CanRun = true
}

func (self *Replayer) Main() {
  instanceId := self.Factory.GetCheckpointInstanceID(self.Config.GetMyGroupIdx()) + 1

  for {
    if self.IsEnd {
      return
    }

    if !self.CanRun {
      self.IsPause = true
      time.Sleep(1000 * time.Microsecond)
      continue
    }

    if instanceId >= self.CpMng.GetMaxChosenInstanceID() {
      time.Sleep(1000 * time.Microsecond)
      continue
    }

    ok := self.PlayOne(instanceId)
    if ok {
      instanceId++
    } else {
      time.Sleep(500 * time.Microsecond)
    }
  }
}

func (self *Replayer) PlayOne(instanceId uint64) bool {
  groupIdx := self.Config.GetMyGroupIdx()

  var state common.AcceptorStateData
  err := self.PaxosLog.ReadState(groupIdx, instanceId, &state)
  if err != nil {
    return false
  }

  ok := self.Factory.ExecuteForCheckpoint(groupIdx, instanceId, state.GetAcceptedValue())
  if !ok {
    log.Error("Checkpoint sm excute fail, instanceid %d", instanceId)
  }

  return ok
}

func (self *Replayer) Pause() {
  self.IsPause = true
}