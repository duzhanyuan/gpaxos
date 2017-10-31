package network

import (
  "net"
  "time"

  log "github.com/lichuang/log4go"
)

type Connection struct {
  addr string

  conn net.Conn
}

func NewConnection(addr string) *Connection {
  return &Connection{
    addr:addr,
    conn:nil,
  }
}

func (self *Connection) Send(msg []byte) bool {
  if self.conn == nil && !self.tryConnect() {
    log.Error("connect to %s fail", self.addr)
    return false
  }

  n, err := self.conn.Write(msg)
  return n == len(msg) && err == nil
}

func (self *Connection) tryConnect() bool {
  maxTry := 3
  for maxTry > 0 {
    conn, err := net.Dial("tcp", self.addr)
    if err != nil {
      log.Error("try to connect to %s error %v", self.addr, err)
      time.Sleep(time.Second * 1)
      maxTry -= 1
      continue
    }
    self.conn = conn
    return true
  }

  return false
}
