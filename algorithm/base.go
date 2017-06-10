package algorithm

import (
    proto "github.com/golang/protobuf/proto"

    "github.com/lichuang/gpaxos/config"
    "github.com/lichuang/gpaxos/common"
    "github.com/lichuang/gpaxos/log"
    "github.com/lichuang/gpaxos/util"
)

const (
    BroadcastMessage_Type_RunSelf_First = iota
    BroadcastMessage_Type_RunSelf_Final
    BroadcastMessage_Type_RunSelf_None
)

type Base struct {
    instanceId uint64
    config     *config.Config
    transport  common.MsgTransport
    instance   *Instance
}

func (self *Base) initBase(config *config.Config, transport common.MsgTransport, instance *Instance) {
    self.config = config
    self.transport = transport
    self.instance = instance
    self.instanceId = 0
}

func (self *Base) GetInstanceId() uint64 {
    return self.instanceId
}

func (self *Base) SetInstanceId(instanceId uint64) {
    self.instanceId = instanceId
}

func (self *Base) NewInstanceId() {
    self.instanceId++
}

func (self *Base) GetLastChecksum() uint32 {
    return self.instance.GetLastChecksum()
}

func (self *Base) PackMsg(paxosMsg common.PaxosMsg, buffer []byte) error {
    body, err:= proto.Marshal(paxosMsg)
    if err != nil {
        log.Error("paxosmsg Marshal fail:%v", err)
        return err
    }

    return self.PackBaseMsg(body, common.MsgCmd_PaxosMsg, buffer)
}

func (self *Base) PackCheckpointMsg(msg common.CheckpointMsg, buffer []byte) error {
    body, err:= proto.Marshal(msg)
    if err != nil {
        log.Error("checkpoint msg Marshal fail:%v", err)
        return err
    }

    return self.PackBaseMsg(body, common.MsgCmd_CheckpointMsg, buffer)
}

func (self *Base) PackBaseMsg(body []byte, cmd int32, buffer []byte) error {
    groupIdx := self.config.GetMyGroupId()
    var groupBuf []byte
    util.EncodeInt32(groupBuf,0,groupIdx)

    header := common.Header{
        Gid:proto.Uint64(self.config.GetGid()),
        Rid:proto.Uint64(0),
        Cmdid:proto.Int32(cmd),
        Version:proto.Int32(1),
    }

    headBuf, err := proto.Marshal(header)
    if err != nil {
        log.Error("header Marshal fail:%v", err)
        return err
    }

    var headerLenBuf [] byte
    util.EncodeUint16(headerLenBuf, 0, uint16(len(headBuf)))

    buffer = util.AppendBytes(groupBuf, headBuf, body)

    ckSum := util.Crc32(0, buffer, common.NET_CRC32SKIP)
    var cksumBuf []byte
    util.EncodeInt32(cksumBuf, 0, ckSum)

    buffer = util.AppendBytes(buffer, cksumBuf)
    return nil
}

func (self *Base) UnpackBaseMsg(buffer []byte, header *common.Header, bodyStartPos *int32, bodyLen *int32) error {

}