package algorithm

import (
	"github.com/lichuang/gpaxos/config"
	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos/log"
	"github.com/lichuang/gpaxos/logstorage"
	"github.com/lichuang/gpaxos/util"
)

type AcceptorState struct {
	PromiseNum BallotNumber
	AcceptedNum BallotNumber
	AcceptValue []byte
	CheckSum 	uint32
	PaxosLog	*logstorage.PaxosLog
	Config		*config.Config
	SyncTimes   int32
}

func NewAcceptorState(config *config.Config, logStorage logstorage.LogStorage) *AcceptorState {
	acceptorState := new(AcceptorState)
	acceptorState.Config = config
	acceptorState.PaxosLog = logstorage.NewPaxosLog(logStorage)
	acceptorState.SyncTimes = 0
	acceptorState.init()

	return acceptorState
}

func (self *AcceptorState) init(){
	self.AcceptedNum.Reset()
	self.CheckSum = 0
}

func (self *AcceptorState) GetPromiseNum() BallotNumber {
	return self.PromiseNum
}

func (self *AcceptorState) SetPromiseNum(promiseNum BallotNumber) {
	self.PromiseNum = promiseNum
}

func (self *AcceptorState) GetAcceptedNum() BallotNumber {
	return self.AcceptedNum
}

func (self *AcceptorState) SetAcceptedNum(acceptedNum BallotNumber) {
	self.AcceptedNum = acceptedNum
}

func (self *AcceptorState) GetAcceptedValue() []byte {
	return self.AcceptValue
}

func (self *AcceptorState) SetAcceptedValue(acceptedValue []byte) {
	self.AcceptValue = acceptedValue
}

func (self *AcceptorState) GetChecksum() uint32 {
	return self.CheckSum
}

func (self *AcceptorState) Persist(instanceid uint64, lastCheckSum uint32) error {
	if instanceid > 0 && lastCheckSum == 0 {
		self.CheckSum = 0
	} else if len(self.AcceptValue) > 0 {
		self.CheckSum = util.Crc32(lastCheckSum, self.AcceptValue, common.CRC32_SKIP)
	}

	var state = common.AcceptorStateData {
		InstanceID: &instanceid,
		PromiseID: &self.PromiseNum.proposalId,
		PromiseNodeID: &self.PromiseNum.nodeId,
		AcceptedID: &self.AcceptedNum.proposalId,
		AcceptedNodeID: &self.AcceptedNum.nodeId,
		AcceptedValue: self.AcceptValue,
		Checksum: &self.CheckSum,
	}

	var options = logstorage.WriteOptions {
		Sync:self.Config.LogSync(),
	}

	if options.Sync {
		self.SyncTimes++
		if self.SyncTimes > self.Config.SyncInterval() {
			self.SyncTimes = 0
		} else {
			options.Sync = false
		}
	}

	err := self.PaxosLog.WriteState(options, self.Config.GetMyGroupId(), instanceid, state)
	if err != nil {
		return err
	}

	log.Info("Groupidx %d instanceid %d promiseid %d promisenodeid %d " +
					 "acceptedid %d acceptednodeid %d valuelen %d cksum %d",
					 self.Config.GetMyGroupId, instanceid, self.PromiseNum.proposalId,
					 self.PromiseNum.nodeId, self.AcceptedNum.proposalId, self.AcceptedNum.nodeId,
					 len(self.AcceptValue), self.CheckSum)
	return nil
}

func (self *AcceptorState)Load(instanceid *uint64) error {
	groupId := self.Config.GetMyGroupId()

	err := self.PaxosLog.GetMaxInstanceIdFromLog(groupId, instanceid)
	if err != nil {
		log.Info("empty database")
		*instanceid = 0
		return nil
	}

	var state common.AcceptorStateData
	err = self.PaxosLog.ReadState(groupId, *instanceid, &state)
	if err != nil {
		return err
	}
	
	self.PromiseNum.proposalId = state.GetPromiseID()
	self.PromiseNum.nodeId = state.GetPromiseNodeID()
	self.AcceptedNum.proposalId = state.GetAcceptedID()
	self.AcceptedNum.nodeId = state.GetAcceptedNodeID()
	self.AcceptValue = state.GetAcceptedValue()
	self.CheckSum = state.GetChecksum()

	log.Info("Groupidx %d instanceid %d promiseid %d promisenodeid %d " +
		"acceptedid %d acceptednodeid %d valuelen %d cksum %d",
		self.Config.GetMyGroupId, instanceid, self.PromiseNum.proposalId,
		self.PromiseNum.nodeId, self.AcceptedNum.proposalId, self.AcceptedNum.nodeId,
		len(self.AcceptValue), self.CheckSum)
	return nil
}