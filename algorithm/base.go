package algorithm

import (
  "github.com/golang/protobuf/proto"

  "encoding/binary"

  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/network"

  log "github.com/lichuang/log4go"
)

const (
  BroadcastMessage_Type_RunSelf_First = iota
  BroadcastMessage_Type_RunSelf_Final
  BroadcastMessage_Type_RunSelf_None
)

var HEADLEN_LEN int32 = int32(binary.Size(uint16(0)))
var CHECKSUM_LEN int32 = int32(binary.Size(uint32(0)))

type Base struct {
  instanceId uint64
  config     *config.Config
  transport  network.Transport
  instance   *Instance
  isTestNode bool
}

func init() {
  HEADLEN_LEN = int32(binary.Size(uint16(0)))
  CHECKSUM_LEN = int32(binary.Size(uint32(0)))
}

func newBase(instance *Instance) *Base {
  return &Base{
    config:instance.config,
    transport:instance.transport,
    instance:instance,
    instanceId:0,
    isTestNode:false,
  }
}

func (self *Base) getInstanceId() uint64 {
  return self.instanceId
}

func (self *Base) setInstanceId(instanceId uint64) {
  self.instanceId = instanceId
}

func (self *Base) newInstance() {
  self.instanceId++
}

func (self *Base) getLastChecksum() uint32 {
  return self.instance.GetLastChecksum()
}

func (self *Base) packPaxosMsg(paxosMsg *common.PaxosMsg) ([]byte, error) {
  body, err := proto.Marshal(paxosMsg)
  if err != nil {
    log.Error("paxos msg Marshal fail:%v", err)
    return nil, err
  }

  return self.packBaseMsg(body, common.MsgCmd_PaxosMsg)
}

func (self *Base) packCheckpointMsg(msg *common.CheckpointMsg) ([]byte, error) {
  body, err := proto.Marshal(msg)
  if err != nil {
    log.Error("checkpoint msg Marshal fail:%v", err)
    return nil, err
  }

  return self.packBaseMsg(body, common.MsgCmd_CheckpointMsg)
}

// format: headerlen(uint16) + header + body + crc32 checksum(uint32)
func (self *Base) packBaseMsg(body []byte, cmd int32) ([]byte, error) {
  header := common.Header{
    Cmdid:   proto.Int32(cmd),
    Version: proto.Int32(common.Version),
  }

  headerBuf, err := proto.Marshal(&header)
  if err != nil {
    log.Error("header Marshal fail:%v", err)
    return nil, err
  }

  headerLenBuf := make([] byte, HEADLEN_LEN)
  util.EncodeUint16(headerLenBuf, 0, uint16(len(headerBuf)))

  buffer := util.AppendBytes(headerLenBuf, headerBuf, body)

  ckSum := util.Crc32(0, buffer, common.NET_CRC32SKIP)
  cksumBuf := make([]byte, CHECKSUM_LEN)
  util.EncodeUint32(cksumBuf, 0, ckSum)

  buffer = util.AppendBytes(buffer, cksumBuf)
  return buffer, nil
}

func (self *Base) unpackBaseMsg(buffer []byte, header *common.Header, bodyStartPos *int32, bodyLen *int32) error {
  return unpackBaseMsg(buffer, header, bodyStartPos, bodyLen)
}

func (self *Base) sendCheckpointMessage(sendToNodeid uint64, msg *common.CheckpointMsg) error {
  if sendToNodeid == self.config.GetMyNodeId() {
    return nil
  }

  buffer, err := self.packCheckpointMsg(msg)
  if err != nil {
    return err
  }

  return self.transport.SendMessage(sendToNodeid, buffer)
}

func (self *Base) sendPaxosMessage(sendToNodeid uint64, msg *common.PaxosMsg) error {
  if sendToNodeid == self.config.GetMyNodeId() {
    return nil
  }

  buffer, err := self.packPaxosMsg(msg)
  if err != nil {
    return err
  }

  return self.transport.SendMessage(sendToNodeid, buffer)
}

func (self *Base) broadcastMessage(msg *common.PaxosMsg, runType int) error {
  if self.isTestNode {
    return nil
  }

  if runType == BroadcastMessage_Type_RunSelf_First {
    err := self.instance.onReceivePaxosMsg(msg, false)
    if err != nil {
      return err
    }
  }

  buffer, err := self.packPaxosMsg(msg)
  if err != nil {
    return err
  }

  err = self.transport.BroadcastMessage(buffer)

  if runType == BroadcastMessage_Type_RunSelf_Final {
    self.instance.onReceivePaxosMsg(msg, false)
  }

  return err
}

/*
func (self *Base) BroadcastMessageToFollower(msg *common.PaxosMsg) error {
  var buffer []byte
  err := self.PackPaxosMsg(msg, buffer)
  if err != nil {
    return err
  }

  return self.transport.BroadcastMessageFollower(buffer)
}

func (self *Base) BroadcastMessageToTempNode(msg *common.PaxosMsg, sendType int) error {
  var buffer []byte
  err := self.PackPaxosMsg(msg, buffer)
  if err != nil {
    return err
  }

  return self.transport.BroadcastMessageTempNode(buffer, sendType)
}
*/

func (self *Base) setAsTestNode() {
  self.isTestNode = true
}

// common function
func unpackBaseMsg(buffer []byte, header *common.Header, bodyStartPos *int32, bodyLen *int32) error {
  var bufferLen int32 = int32(len(buffer))

  var headerLen uint16
  util.DecodeUint16(buffer, 0, &headerLen)

  headerStartPos := HEADLEN_LEN
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

  log.Debug("buffer size %d,header len %d, cmd %d "+ "version %d, body startpos %d",
    bufferLen, headerLen, header.GetCmdid(), header.GetVersion(), *bodyStartPos)

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
