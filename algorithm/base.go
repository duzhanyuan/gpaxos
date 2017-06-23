package algorithm

import (
  "github.com/golang/protobuf/proto"

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

var GROUPIDXLEN int32 = 8
var HEADLEN_LEN int32 = 4
var CHECKSUM_LEN int32 = 8

type Base struct {
  instanceId uint64
  config     *config.Config
  transport  common.MsgTransport
  instance   *Instance
  isTestNode bool
}

func init() {
  GROUPIDXLEN = int32(util.INT32SIZE)
  HEADLEN_LEN = int32(util.UINT16SIZE)
  CHECKSUM_LEN = int32(util.UINT32SIZE)
}

func newBase(config *config.Config, transport *common.MsgTransport, instance *Instance) Base {
  return Base{
    config:config,
    transport:transport,
    instance:instance,
    instanceId:0,
    isTestNode:false,
  }
}

func (self *Base) GetInstanceId() uint64 {
  return self.instanceId
}

func (self *Base) SetInstanceId(instanceId uint64) {
  self.instanceId = instanceId
}

func (self *Base) NewInstance() {
  self.instanceId++
}

func (self *Base) GetLastChecksum() uint32 {
  return self.instance.GetLastChecksum()
}

func (self *Base) PackPaxosMsg(paxosMsg common.PaxosMsg, buffer []byte) error {
  body, err := proto.Marshal(paxosMsg)
  if err != nil {
    log.Error("paxos msg Marshal fail:%v", err)
    return err
  }

  return self.PackBaseMsg(body, common.MsgCmd_PaxosMsg, buffer)
}

func (self *Base) PackCheckpointMsg(msg common.CheckpointMsg, buffer []byte) error {
  body, err := proto.Marshal(msg)
  if err != nil {
    log.Error("checkpoint msg Marshal fail:%v", err)
    return err
  }

  return self.PackBaseMsg(body, common.MsgCmd_CheckpointMsg, buffer)
}

func (self *Base) PackBaseMsg(body []byte, cmd int32, buffer []byte) error {
  groupIdx := self.config.GetMyGroupIdx()
  groupIdBuf := make([]byte, GROUPIDXLEN)
  util.EncodeInt32(groupIdBuf, 0, groupIdx)

  header := common.Header{
    Gid:     proto.Uint64(self.config.GetGid()),
    Rid:     proto.Uint64(0),
    Cmdid:   proto.Int32(cmd),
    Version: proto.Int32(common.Version),
  }

  headerBuf, err := proto.Marshal(header)
  if err != nil {
    log.Error("header Marshal fail:%v", err)
    return err
  }

  headerLenBuf := make([] byte, HEADLEN_LEN)
  util.EncodeUint16(headerLenBuf, 0, uint16(len(headerBuf)))

  buffer = util.AppendBytes(groupIdBuf, headerLenBuf, headerBuf, body)

  ckSum := util.Crc32(0, buffer, common.NET_CRC32SKIP)
  cksumBuf := make([]byte, CHECKSUM_LEN)
  util.EncodeUint32(cksumBuf, 0, ckSum)

  buffer = util.AppendBytes(buffer, cksumBuf)
  return nil
}

func (self *Base) UnpackBaseMsg(buffer []byte, header *common.Header, bodyStartPos *int32, bodyLen *int32) error {
  var bufferLen int32 = int32(len(buffer))

  var headerLen uint16
  util.DecodeUint16(buffer, int(GROUPIDXLEN), &headerLen)

  headerStartPos := GROUPIDXLEN + HEADLEN_LEN
  *bodyStartPos = int32(headerStartPos + int32(headerLen))

  if *bodyStartPos > bufferLen {
    log.Error("header headerlen too long %d", headerLen)
    return common.ErrInvalidMsg
  }

  err := proto.Unmarshal(buffer[headerStartPos:], header)
  if err != nil {
    log.Error("header unmarshal fail:%v", err)
    return common.ErrInvalidMsg
  }

  log.Debug("buffer size %d,header len %d, cmd %d, gid %d, rid %d, "+
    "version %d, body startpos %d",
    bufferLen, headerLen, header.GetCmdid(), header.GetGid(), header.GetRid(),
    header.GetVersion(), *bodyStartPos)

  if *bodyStartPos+int32(CHECKSUM_LEN) > bufferLen {
    log.Error("no checksum, body start pos %d, buffersize %d", *bodyStartPos, bufferLen)
    return common.ErrInvalidMsg
  }

  *bodyLen = bufferLen - int32(CHECKSUM_LEN) - *bodyStartPos

  var cksum uint32
  util.DecodeUint32(buffer, int(bufferLen-CHECKSUM_LEN), &cksum)

  calCksum := util.Crc32(0, buffer[:bufferLen-CHECKSUM_LEN], common.NET_CRC32SKIP)
  if calCksum != cksum {
    log.Error("data bring cksum %d not equal to cal cksum %d", cksum, calCksum)
    return common.ErrInvalidMsg
  }

  return nil
}

func (self *Base) SendCheckpointMessage(sendToNodeid uint64, msg common.CheckpointMsg, sendType int) error {
  if sendToNodeid == self.config.GetMyNodeId() {
    return nil
  }

  var buffer []byte
  err := self.PackCheckpointMsg(msg, buffer)
  if err != nil {
    return err
  }

  return self.transport.SendMessage(sendToNodeid, buffer, sendType)
}

func (self *Base) SendPaxosMessage(sendToNodeid uint64, msg common.PaxosMsg, sendType int) error {
  if sendToNodeid == self.config.GetMyNodeId() {
    return nil
  }

  var buffer []byte
  err := self.PackPaxosMsg(msg, buffer)
  if err != nil {
    return err
  }

  return self.transport.SendMessage(sendToNodeid, buffer, sendType)
}

func (self *Base) BroadcastMessage(msg common.PaxosMsg, runType int, sendType int) error {
  if self.isTestNode {
    return nil
  }

  if runType == BroadcastMessage_Type_RunSelf_First {
    err := self.instance.OnReceivePaxosMsg(msg)
    if err != nil {
      return err
    }
  }

  var buffer []byte
  err := self.PackPaxosMsg(msg, buffer)
  if err != nil {
    return err
  }

  err = self.transport.BroadcastMessage(buffer, sendType)

  if runType == BroadcastMessage_Type_RunSelf_Final {
    self.instance.OnReceivePaxosMsg(msg)
  }

  return err
}

func (self *Base) BroadcastMessageToFollower(msg common.PaxosMsg, sendType int) error {
  var buffer []byte
  err := self.PackPaxosMsg(msg, buffer)
  if err != nil {
    return err
  }

  return self.transport.BroadcastMessageFollower(buffer, sendType)
}

func (self *Base) BroadcastMessageToTempNode(msg common.PaxosMsg, sendType int) error {
  var buffer []byte
  err := self.PackPaxosMsg(msg, buffer)
  if err != nil {
    return err
  }

  return self.transport.BroadcastMessageTempNode(buffer, sendType)
}

func (self *Base) SetAsTestNode() {
  self.isTestNode = true
}
