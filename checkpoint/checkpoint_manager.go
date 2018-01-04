package checkpoint

import (
	"github.com/lichuang/gpaxos/config"
	"github.com/lichuang/gpaxos/statemachine"
	"github.com/lichuang/gpaxos/storage"
	"github.com/lichuang/gpaxos/util"

	log "github.com/lichuang/log4go"
)

type CheckpointManager struct {
	config *config.Config
	logStorage *storage.LogStorage
	factory *statemachine.StatemachineFactory
	cleaner *Cleaner
	replayer *Replayer

	minChosenInstanceId uint64
	maxChosenInstanceId uint64
	inAskforCheckpointMode bool
	useCheckpointReplayer bool

	needAskSet map[uint64]bool
	lastAskforCheckpointTime uint64
}

func NewCheckpointManager(config *config.Config, factory *statemachine.StatemachineFactory,
	logStorage *storage.LogStorage, useReplayer bool) *CheckpointManager {
	mnger := &CheckpointManager{
		config: config,
		logStorage:logStorage,
		factory:factory,
		useCheckpointReplayer: useReplayer,
	}

	mnger.cleaner = NewCleaner(config, factory, logStorage, mnger)
	if useReplayer {
		mnger.replayer = NewReplayer(config, factory, logStorage, mnger)
	}
	return mnger
}

func (self *CheckpointManager) Init() error {
	instanceId, err := self.logStorage.GetMinChosenInstanceID()
	if err != nil {
		return err
	}

	self.minChosenInstanceId = instanceId
	err = self.cleaner.FixMinChosenInstanceID(self.minChosenInstanceId)
	if err != nil {
		return err
	}
	return nil
}

func (self *CheckpointManager) Stop() {
	if self.useCheckpointReplayer {
		self.replayer.Stop()
	}
	self.cleaner.Stop()
}

func (self *CheckpointManager) PrepareForAskforCheckpoint(sendNodeId uint64) error {
	self.needAskSet[sendNodeId] = true
	if self.lastAskforCheckpointTime == 0 {
		self.lastAskforCheckpointTime = util.NowTimeMs()
	}

	now := util.NowTimeMs()
	if now >= self.lastAskforCheckpointTime + 60000 {

	} else {
		if len(self.needAskSet) < self.config.GetMajorityCount() {

		}
	}

	self.lastAskforCheckpointTime = 0
	self.inAskforCheckpointMode = true

	return nil
}

func (self *CheckpointManager) GetMinChosenInstanceID() uint64 {
	return self.minChosenInstanceId
}

func (self *CheckpointManager) GetMaxChosenInstanceID() uint64 {
	return self.maxChosenInstanceId
}

func (self *CheckpointManager) SetMinChosenInstanceID(instanceId uint64) error {
	/*
	options := storage.WriteOptions{
		Sync:true,
	}
	*/
	err := self.logStorage.SetMinChosenInstanceID(instanceId)
	if err != nil {
		return err
	}

	self.minChosenInstanceId = instanceId
	return nil
}