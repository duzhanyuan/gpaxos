package checkpoint

import (
	"github.com/lichuang/gpaxos/config"
	"github.com/lichuang/gpaxos/statemachine"
	"github.com/lichuang/gpaxos/storage"
	"github.com/lichuang/gpaxos/util"
	"time"

	log "github.com/lichuang/log4go"
)

type Replayer struct {
	config *config.Config
	paxosLog *storage.PaxosLog
	factory *statemachine.StatemachineFactory
	ckmnger *CheckpointManager

	isPaused bool
	isEnd bool
	isStart bool
	canRun bool
}

func NewReplayer(config *config.Config, factory *statemachine.StatemachineFactory,
	logStorage *storage.LogStorage, mnger *CheckpointManager) *Replayer {
	replayer := &Replayer{
		config: config,
		paxosLog:storage.NewPaxosLog(logStorage),
		factory:factory,
		ckmnger:mnger,
	}

	util.StartRoutine(replayer.main)
	return replayer
}

func (self *Replayer) Stop() {
	self.isEnd = true
}

func (self *Replayer) Pause() {
	self.canRun = false
}

func (self *Replayer) main() {
	instanceId := self.factory.GetCheckpointInstanceID() + 1

	for {
		if self.isEnd {
			break
		}

		if !self.canRun {
			self.isPaused = true
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if instanceId >= self.ckmnger.GetMaxChosenInstanceID() {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		err := self.PlayOne(instanceId)
		if err == nil {
			instanceId+=1
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (self *Replayer) PlayOne(instanceId uint64) error {
	state, err := self.paxosLog.ReadState(instanceId)
	if err != nil {
		return err
	}

	err = self.factory.ExecuteForCheckpoint(instanceId, state.GetAcceptedValue())
	if err != nil {
		log.Error("checkpoint sm execute fail:%v, instanceid:%d", err, instanceId)
		return err
	}

	return nil
}