package algorithm

import (
	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos/util"
)

type AcceptorState struct {
	PromiseNum BallotNumber
	AcceptedNum BallotNumber
	AcceptValue common.Value
	CheckSum 		uint32
}

func (as *AcceptorState) Init(){
	as.AcceptedNum.Reset()
	as.AcceptValue = ""
}

func (as *AcceptorState) Persist(instanceid common.InstanceId, lastCheckSum uint32) {
	if instanceid > 0 && lastCheckSum == 0 {
		as.CheckSum = 0
	} else if len(as.AcceptValue) > 0 {
		as.CheckSum = util.Crc32(lastCheckSum, as.AcceptValue, common.CRC32_SKIP)
	}

	var state common.AcceptorStateData {
		InstanceID: instanceid,
		PromiseID: as.PromiseNum.proposalId,
		PromiseNodeID: as.PromiseNum.nodeId,
		AcceptedID: as.AcceptedNum.proposalId,
		AcceptedNodeID: as.AcceptedNum.nodeId,
		AcceptedValue: as.AcceptValue,
		Checksum: as.CheckSum
	}

	
}

func NewAcceptorState() {
	state := new(AcceptorState)
}