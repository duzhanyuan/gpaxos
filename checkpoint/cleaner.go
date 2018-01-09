package checkpoint

import (
	"github.com/lichuang/gpaxos/config"
	"github.com/lichuang/gpaxos/statemachine"
	"github.com/lichuang/gpaxos/storage"
	"github.com/lichuang/gpaxos/util"
	"time"
	"github.com/lichuang/gpaxos/common"

	log "github.com/lichuang/log4go"
)

const (
	DELETE_SAVE_INTERVAL = 10
)

type Cleaner struct {
	config *config.Config
	logStorage *storage.LogStorage
	factory *statemachine.StatemachineFactory
	ckmnger *CheckpointManager

	holdCount uint64
	lastSaveInstanceId uint64

	isPaused bool
	isEnd bool
	isStart bool
	canRun bool
}

func NewCleaner(config *config.Config, factory *statemachine.StatemachineFactory,
								logStorage *storage.LogStorage, mnger *CheckpointManager) *Cleaner {
	cleaner := &Cleaner{
		config: config,
		logStorage:logStorage,
		factory:factory,
		ckmnger:mnger,
		holdCount: 100000,
	}

	return cleaner
}

func (self *Cleaner) Start() {
	util.StartRoutine(self.main)
}

func (self *Cleaner) Stop() {
	self.isEnd = true
}

func (self *Cleaner) Pause() {
	self.canRun = false
}

func (self *Cleaner) main() {
	self.isStart = true
	self.Continue()

	deleteQps := common.GetCleanerDeleteQps()
	sleepMs := 1
	deleteInterval := 1
	if deleteQps < 1000 {
		sleepMs = int(1000 / deleteQps)
	} else {
		deleteInterval = int(deleteQps / 1000) + 1
	}

	deleteCnt := 0
	for {
		if self.isEnd {
			break
		}

		if !self.canRun {
			self.isPaused = true
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		instanceId := self.ckmnger.GetMinChosenInstanceID()
		maxInstanceId := self.ckmnger.GetMinChosenInstanceID()
		cpInstanceId := self.factory.GetCheckpointInstanceID() + 1
		for instanceId + self.holdCount < cpInstanceId && instanceId + self.holdCount < maxInstanceId {
			err := self.DeleteOne(instanceId)
			if err != nil {
				log.Error("delete system fail, instanceid %d", instanceId)
				break
			}

			instanceId += 1
			deleteCnt  += 1
			if deleteCnt >= deleteInterval {
				deleteCnt = 0
				time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			}
		}

		if cpInstanceId == 0 {
			log.Info("sleep a while, max deleted instanceid %d checkpoint instanceid(no checkpoint) now instance id %d",
				instanceId, self.ckmnger.GetMaxChosenInstanceID())
		} else {
			log.Info("sleep a while, max deleted instanceid %d checkpoint instanceid %d now instance id %d",
				instanceId, cpInstanceId, self.ckmnger.GetMaxChosenInstanceID())
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (self *Cleaner) FixMinChosenInstanceID(oldMinChosenInstanceId uint64) error {
	cpInstanceId := self.factory.GetCheckpointInstanceID() + 1
	fixMinChosenInstanceId := oldMinChosenInstanceId

	for instanceId := oldMinChosenInstanceId; instanceId < oldMinChosenInstanceId +DELETE_SAVE_INTERVAL; instanceId++ {
		if instanceId >= cpInstanceId {
			break
		}

		_, err := self.logStorage.Get(instanceId)

		if err == nil {
			fixMinChosenInstanceId = instanceId + 1
		}
	}

	if fixMinChosenInstanceId > oldMinChosenInstanceId {
		err := self.ckmnger.SetMinChosenInstanceID(fixMinChosenInstanceId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Cleaner) DeleteOne(instanceId uint64) error {
	options := storage.WriteOptions{
		Sync:false,
	}

	err := self.logStorage.Del(options, instanceId)
	if err != nil {
		return err
	}

	self.ckmnger.SetMinChosenInstanceID(instanceId)
	if instanceId >= self.lastSaveInstanceId +DELETE_SAVE_INTERVAL {
		err := self.ckmnger.SetMinChosenInstanceID(instanceId + 1)
		if err != nil {
			log.Error("SetMinChosenInstanceID fail, now delete instanceid %d", instanceId)
			return err
		}
		self.lastSaveInstanceId = instanceId

		log.Info("delete %d instance done, now minchosen instanceid %d", DELETE_SAVE_INTERVAL, instanceId + 1)
	}

	return nil
}

func (self *Cleaner) Continue() {
	self.isPaused = false
	self.canRun = true
}