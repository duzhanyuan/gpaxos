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

var HEADLEN_LEN = int32(binary.Size(uint16(0)))
var CHECKSUM_LEN = int32(binary.Size(uint32(0)))

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

func newBase(instance *Instance) Base {
  return Base{
    config:instance.config,
    transport:instance.transport,
    instance:instance,
    instanceId:1,
    isTestNode:false,
  }
}

func (self *Base) GetInstanceId() uint64 {
  return self.instanceId
}

func (self *Base) setInstanceId(instanceId uint64) {
  self.instanceId = instanceId
}

func (self *Base) newInstance() {
  self.instanceId++
}

func (self *Base) GetLastChecksum() uint32 {
  return self.instance.GetLastChecksum()
}

func (self *Base) packPaxosMsg(paxosMsg *common.PaxosMsg) ([]byte, *common.Header, error) {
  body, err := proto.Marshal(paxosMsg)
  if err != nil {
    log.Error("paxos msg Marshal fail:%v", err)
    return nil, nil, err
  }

  return self.packBaseMsg(body, common.MsgCmd_PaxosMsg)
}

func (self *Base) packCheckpointMsg(msg *common.CheckpointMsg) ([]byte, *common.Header,error) {
  body, err := proto.Marshal(msg)
  if err != nil {
    log.Error("checkpoint msg Marshal fail:%v", err)
    return nil, nil, err
  }

  return self.packBaseMsg(body, common.MsgCmd_CheckpointMsg)
}

// format: headerlen(uint16) + header + body + crc32 checksum(uint32)
func (self *Base) packBaseMsg(body []byte, cmd int32) (buffer []byte, header *common.Header, err error) {
  h := &common.Header{
    Cmdid:   proto.Int32(cmd),
    // buffer len + checksum len
    Bodylen: proto.Int32(int32(len(body) + util.UINT32SIZE)),
    Version: proto.Int32(common.Version),
  }
  header = h

  headerBuf, err := proto.Marshal(header)
  if err != nil {
    log.Error("header Marshal fail:%v", err)
    return
  }

  headerLenBuf := make([] byte, HEADLEN_LEN)
  util.EncodeUint16(headerLenBuf, 0, uint16(len(headerBuf)))

  buffer = util.AppendBytes(headerLenBuf, headerBuf, body)

  ckSum := util.Crc32(0, buffer, common.NET_CRC32SKIP)
  cksumBuf := make([]byte, CHECKSUM_LEN)
  util.EncodeUint32(cksumBuf, 0, ckSum)

  buffer = util.AppendBytes(buffer, cksumBuf)

  return
}

func (self *Base) sendCheckpointMessage(sendToNodeid uint64, msg *common.CheckpointMsg) error {
  if sendToNodeid == self.config.GetMyNodeId() {
    return nil
  }

  buffer, _, err := self.packCheckpointMsg(msg)
  if err != nil {
    return err
  }

  return self.transport.SendMessage(sendToNodeid, buffer)
}

func (self *Base) sendPaxosMessage(sendToNodeid uint64, msg *common.PaxosMsg) error {
  if sendToNodeid == self.config.GetMyNodeId() {
    log.Error("no need send to self")
    return nil
  }

  buffer, _, err := self.packPaxosMsg(msg)
  if err != nil {
    log.Error("pack paxos msg error %v", err)
    return err
  }

  return self.transport.SendMessage(sendToNodeid, buffer)
}

func (self *Base) broadcastMessage(msg *common.PaxosMsg, runType int) error {
  if self.isTestNode {
    return nil
  }

  if runType == BroadcastMessage_Type_RunSelf_First {
    err := self.instance.OnReceivePaxosMsg(msg, false)
    if err != nil {
      return err
    }
  }

  buffer, _, err := self.packPaxosMsg(msg)
  if err != nil {
    return err
  }

  err = self.transport.BroadcastMessage(buffer)

  if runType == BroadcastMessage_Type_RunSelf_Final {
    self.instance.OnReceivePaxosMsg(msg, false)
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
func unpackBaseMsg(buffer []byte, header *common.Header) (body []byte, err error) {
  var bufferLen = int32(len(buffer))

  bodyStartPos := bufferLen - header.GetBodylen()

  log.Debug("buffer size %d, cmd %d "+ "version %d, body startpos %d",
    bufferLen, header.GetCmdid(), header.GetVersion(), bodyStartPos)

  if bodyStartPos+int32(CHECKSUM_LEN) > bufferLen {
    log.Error("no checksum, body start pos %d, buffersize %d", bodyStartPos, bufferLen)
    err = common.ErrInvalidMsg
    return
  }

  var cksum uint32
  util.DecodeUint32(buffer, int(bufferLen-CHECKSUM_LEN), &cksum)

  calCksum := util.Crc32(0, buffer[:bufferLen-CHECKSUM_LEN], common.NET_CRC32SKIP)
  if calCksum != cksum {
    log.Error("data bring cksum %d not equal to cal cksum %d", cksum, calCksum)
    err = common.ErrInvalidMsg
    return
  }

  body = buffer[bodyStartPos:header.GetBodylen() + bodyStartPos - int32(util.UINT32SIZE)]
  err = nil
  return
}
