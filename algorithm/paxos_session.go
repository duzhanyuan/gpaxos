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

type PaxosSessionFactory struct {

}

func NewPaxosSessionFactory() *PaxosSessionFactory{
  return &PaxosSessionFactory{}
}

func (self *PaxosSessionFactory) Create(conn net.Conn) network.Session {
  return newPaxosSession(conn)
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
  bufLen int
  bodyStartPos int32
}

func newPaxosSession(conn net.Conn) *PaxosSession {
  return &PaxosSession{
    conn:conn,
    defaultBuf:make([] byte, DEFAULT_BUF_SIZE),
    bufLen:DEFAULT_BUF_SIZE,
  }
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

  self.state = ACCEPTING_BODY
  return nil
}


func (self *PaxosSession) recvBody() error {
  conn := self.conn
  bodyLen := self.header.GetBodylen()
  var body [] byte
  body = self.defaultBuf
  endPos := self.bodyStartPos + bodyLen
  io.ReadFull(conn, body[self.bodyStartPos:endPos])
  buf, _ := unpackBaseMsg(self.defaultBuf[:endPos], &self.header)
  log.Debug("body:%s", string(buf))

  return nil
}
/*
func (self *PaxosSession) recvBody() error {
  conn := self.conn
  bodyLen := self.header.GetBodylen()
  var body [] byte
  if bodyLen > DEFAULT_BUF_SIZE {
    body = make([]byte, bodyLen)
  } else {
    body = self.defaultBuf
  }
  endPos := self.bodyStartPos + bodyLen
  n, err := io.ReadFull(conn, body[self.bodyStartPos:endPos])
  if n != int(bodyLen) || err != nil {
    log.Error("recv body error from %s: %v", conn.RemoteAddr().String(), err)
    return common.ErrInvalidMsg
  }

  self.body = body[self.bodyStartPos:endPos - int32(util.UINT32SIZE)]

  crcCksumbuf := body[endPos - int32(util.UINT32SIZE) : endPos]
  var cksum uint32
  util.DecodeUint32(crcCksumbuf, 0, &cksum)

  calCksum := util.Crc32(0, self.defaultBuf[util.UINT16SIZE:endPos - int32(util.UINT32SIZE)], common.NET_CRC32SKIP)
  if cksum != calCksum {
    log.Error("recv invalid body from %s", conn.RemoteAddr().String())
    return common.ErrInvalidMsg
  }

  self.state = ACCEPTING_HEADER
  log.Error("body: %s\n", string(self.body))
  return nil
}
*/

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
