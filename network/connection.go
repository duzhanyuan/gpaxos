package network

import (
  "net"
  "time"

  log "github.com/lichuang/log4go"
)

type Connection struct {
  addr string

  conn net.Conn

  nodeId uint64
}

func NewConnection(addr string, nodeId uint64) *Connection {
  return &Connection{
    addr:addr,
    conn:nil,
    nodeId:nodeId,
  }
}

func (self *Connection) Send(msg []byte) bool {
  //log.Debug("send to %d", self.nodeId)
  if self.conn == nil && !self.tryConnect() {
    log.Error("connect to %s fail", self.addr)
    return false
  }

  n, err := self.conn.Write(msg)
  if err != nil {
    log.Error("send error %v", err)
  }
  return n == len(msg) && err == nil
}

func (self *Connection) tryConnect() bool {
  maxTry := 1
  for maxTry > 0 {
    conn, err := net.Dial("tcp", self.addr)
    if err != nil {
      log.Error("try to connect to %s error %v", self.addr, err)
      time.Sleep(time.Second * 1)
      maxTry -= 1
      if maxTry == 0{
        return false
      }
      continue
    }
    self.conn = conn
    return true
  }

  return false
}
