package algorithm

import (
	"github.com/lichuang/gpaxos/config"
	"github.com/lichuang/gpaxos/statemachine"
	"github.com/lichuang/gpaxos/checkpoint"
	"github.com/lichuang/gpaxos/util"
	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos"
	log "github.com/lichuang/log4go"
	"time"
	"os"
)

const tmpBufferLen = 102400

type CheckpointSender struct {
	sendNodeId uint64
	config *config.Config
	learner *Learner
	factory *statemachine.StatemachineFactory
	ckMnger *checkpoint.CheckpointManager
	uuid uint64
	sequence uint64
	isEnd bool
	isEnded bool
	ackSequence uint64
	absLastAckTime uint64
	alreadySendedFile map[string]bool
	tmpBuffer []byte
}

func NewCheckpointSender(sendNodeId uint64, config *config.Config, learner *Learner,
	 											 factory *statemachine.StatemachineFactory,
 											 	ckmnger *checkpoint.CheckpointManager) *CheckpointSender {
	cksender := &CheckpointSender{
		sendNodeId:sendNodeId,
		config:config,
		learner:learner,
		factory:factory,
		ckMnger:ckmnger,
		uuid:config.GetMyNodeId() ^ learner.GetInstanceId() + uint64(util.Rand()),
		tmpBuffer: make([]byte, tmpBufferLen),
	}

	util.StartRoutine(cksender.main)
	return cksender
}

func (self *CheckpointSender) Stop() {
	if !self.isEnded {
		self.isEnd = true
	}
}

func (self *CheckpointSender) IsEnd() bool {
	return self.isEnded
}

func (self *CheckpointSender) End() {
	self.isEnd = true
}

func (self *CheckpointSender) main() {
	self.absLastAckTime = util.NowTimeMs()

	needContinue := false
	for !self.ckMnger.GetRelayer().IsPaused() {
		if self.isEnd {
			self.isEnded = true
			return
		}

		needContinue = true
		self.ckMnger.GetRelayer().Pause()
		log.Debug("wait replayer pause")
		util.SleepMs(100)
	}

	err := self.LockCheckpoint()
	if err == nil {
		self.SendCheckpoint()
		self.UnlockCheckpoint()
	}

	if needContinue {
		self.ckMnger.GetRelayer().Continue()
	}

	log.Info("Checkpoint.Sender [END]")
	self.isEnded = true
}

func (self *CheckpointSender) LockCheckpoint() error {
	smList := self.factory.GetSMList()
	lockSmList := make([] gpaxos.StateMachine, 0)

	var err error
	for _, sm := range smList {
		err = sm.LockCheckpointState()
		if err != nil {
			break
		}
		lockSmList = append(lockSmList, sm)
	}

	if err != nil {
		for _, sm := range lockSmList {
			sm.UnLockCheckpointState()
		}
	}

	return err
}

func (self *CheckpointSender) UnlockCheckpoint() {
	smList := self.factory.GetSMList()

	for _, sm := range smList {
		sm.UnLockCheckpointState()
	}
}

func (self *CheckpointSender) SendCheckpoint() error {
	learner := self.learner
	err := learner.SendCheckpointBegin(self.sendNodeId, self.uuid, self.sequence, self.factory.GetCheckpointInstanceID())
	if err != nil {
		log.Error("SendCheckpoint fail: %v", err)
		return err
	}

	self.sequence += 1

	smList := self.factory.GetSMList()
	for _, sm := range smList {
		err = self.SendCheckpointForSM(sm)
		if err != nil {
			return err
		}
	}

	err = learner.SendCheckpointEnd(self.sendNodeId, self.uuid, self.sequence, self.factory.GetCheckpointInstanceID())
	if err != nil {
		log.Error("SendCheckpointEnd fail: %v", err)
	}

	return err
}

func (self *CheckpointSender) SendCheckpointForSM(statemachine gpaxos.StateMachine) error {
	var dirPath string
	var fileList = make([]string, 0)

	err := statemachine.GetCheckpointState(&dirPath, fileList)
	if err != nil {
		return err
	}

	if len(dirPath) == 0 {
		return nil
	}

	if dirPath[len(dirPath) - 1] != '/' {
		dirPath += "/"
	}

	for _, file := range fileList {
		err = self.SendFile(statemachine, dirPath, file)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *CheckpointSender) SendFile(statemachine gpaxos.StateMachine, dir string, file string) error {
	path := dir + file

	_, exist := self.alreadySendedFile[path]
	if exist {
		return nil
	}

	fd, err := os.Open(path)
	if err != nil {
		return err
	}

	var offset uint64 = 0
	for {
		readLen, err := fd.Read(self.tmpBuffer)

		if err != nil {
			fd.Close()
			return err
		}
		if readLen == 0 {
			break
		}

		err = self.SendBuffer(statemachine.SMID(), statemachine.GetCheckpointInstanceID(),
													path, offset, self.tmpBuffer, readLen)
		if err != nil {
			fd.Close()
			return err
		}

		if readLen < tmpBufferLen {
			return nil
		}

		offset += uint64(readLen)
	}

	self.alreadySendedFile[path] = true
	fd.Close()
	return nil
}

func (self *CheckpointSender)SendBuffer(smid int32, ckInstanceId uint64, file string,
																				offser uint64, buffer []byte, bufLen int) error {
	ckSum := util.Crc32(0, buffer[:bufLen], common.CRC32_SKIP)

	for {
		if self.isEnd {
			return nil
		}

		err := self.CheckAck(self.sequence)
		if err != nil {
			return err
		}

		err = self.learner.SendCheckpoint(self.sendNodeId, self.uuid, self.sequence,
																		  ckInstanceId, ckSum, file, smid, offser, buffer)
		if err != nil {
			util.SleepMs(30000)
		} else {
			self.sequence += 1
			break
		}
	}

	return nil
}

func (self *CheckpointSender) Ack(sendNodeId uint64, uuid uint64, sequence uint64) {
	if sendNodeId != self.sendNodeId {
		return
	}

	if self.uuid != uuid {
		return
	}

	if self.ackSequence != sequence {
		return
	}

	self.ackSequence += 1
	self.absLastAckTime  = util.NowTimeMs()
}

func (self *CheckpointSender) CheckAck(sendSequence uint64) error {
	for sendSequence > self.ackSequence + 100 {
		now := util.NowTimeMs()
		var passTime uint64
		if now > self.absLastAckTime {
			passTime = now - self.absLastAckTime
		}

		if self.isEnd {

		}

		if passTime > 200 {

		}

		time.Sleep(20 * time.Millisecond)
	}

	return nil
}