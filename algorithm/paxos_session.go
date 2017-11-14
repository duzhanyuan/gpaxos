package algorithm

import (
  "net"
  "github.com/lichuang/gpaxos/network"
  "github.com/lichuang/gpaxos/util"
  "io"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos/common"
  "github.com/golang/protobuf/proto"
)

const DEFAULT_BUF_SIZE = 1024

type MsgHandler interface {
  OnReceiveMsg(msg []byte, cmd int32) error
}

type PaxosSessionFactory struct {
  handler MsgHandler
}

func NewPaxosSessionFactory(handler MsgHandler) *PaxosSessionFactory{
  return &PaxosSessionFactory{
    handler:handler,
  }
}

func (self *PaxosSessionFactory) Create(conn net.Conn) network.Session {
  session := newPaxosSession(conn)
  session.handler = self.handler
  return session
}

const (
  ACCEPTING_HEADER = iota
  ACCEPTING_BODY

)

type PaxosSession struct {
  conn net.Conn
  header common.Header
  state int
  defaultBuf []byte
  body []byte
  msgBuf []byte
  bufLen int
  bodyStartPos int32
  handler MsgHandler
}

func newPaxosSession(conn net.Conn) *PaxosSession {
  return &PaxosSession{
    conn:conn,
    defaultBuf:make([] byte, DEFAULT_BUF_SIZE),
    bufLen:DEFAULT_BUF_SIZE,
    state:ACCEPTING_HEADER,
  }
}

func (self *PaxosSession) isInvalidHeader() bool {
  cmd := self.header.GetCmdid()
  if cmd == common.MsgCmd_PaxosMsg {
    return true
  }

  if cmd == common.MsgCmd_CheckpointMsg {
    return true
  }

  log.Error("invalid cmd id %d from %s", cmd, self.conn.RemoteAddr())
  return false
}

func (self *PaxosSession) recvHeader() error {
  conn := self.conn
  n, err := io.ReadFull(conn, self.defaultBuf[:util.UINT16SIZE])
  if n != util.UINT16SIZE || err != nil{
    log.Error("header error from %s: %v", conn.RemoteAddr().String(), err)
    return common.ErrInvalidMsg
  }

  var headerLen uint16
  util.DecodeUint16(self.defaultBuf[:], 0, &headerLen)

  self.bodyStartPos = int32(util.UINT16SIZE) + int32(headerLen)
  n, err = io.ReadFull(conn, self.defaultBuf[util.UINT16SIZE:self.bodyStartPos])
  if n != int(headerLen) || err != nil{
    log.Error("header error from %s: %v", conn.RemoteAddr().String(), err)
    return common.ErrInvalidMsg
  }

  err = proto.Unmarshal(self.defaultBuf[util.UINT16SIZE:self.bodyStartPos], &self.header)
  if err != nil {
    log.Error("header error from %s: %v", conn.RemoteAddr().String(), err)
    return common.ErrInvalidMsg
  }

  if !self.isInvalidHeader() {
    return common.ErrInvalidMsg
  }

  self.state = ACCEPTING_BODY
  return nil
}

func (self *PaxosSession) dispatchMsg() error {
  return self.handler.OnReceiveMsg(self.msgBuf, self.header.GetCmdid())
}

func (self *PaxosSession) recvBody() error {
  conn := self.conn
  bodyLen := self.header.GetBodylen()
  var body [] byte
  body = self.defaultBuf
  endPos := self.bodyStartPos + bodyLen
  n, err := io.ReadFull(conn, body[self.bodyStartPos:endPos])
  if n != int(bodyLen) || err != nil {
    log.Error("recv body error from %s: %v", conn.RemoteAddr().String(), err)
    return common.ErrInvalidMsg
  }
  buf, err := unpackBaseMsg(self.defaultBuf[:endPos], &self.header)
  if err != nil {
    log.Error("recv body error from %s: %v", conn.RemoteAddr().String(), err)
    return common.ErrInvalidMsg
  }
  self.msgBuf = buf
  log.Debug("body:%s", string(self.msgBuf))
  err = self.dispatchMsg()
  if err != nil {
    log.Error("handle msg from %s error %v", self.conn.RemoteAddr(), err)
    return err
  }

  self.state = ACCEPTING_HEADER
  return nil
}

func (self *PaxosSession) Handle() {
  conn := self.conn
  var err error = nil

  for err == nil {
    state := self.state
    switch state {
    case ACCEPTING_HEADER:
      err = self.recvHeader()
      break
    case ACCEPTING_BODY:
      err = self.recvBody()
      break
    }
  }

  conn.Close()
}
