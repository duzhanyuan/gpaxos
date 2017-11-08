package algorithm

import (
  "net"
  "github.com/lichuang/gpaxos/network"
)

type PaxosSessionFactory struct {

}

func NewPaxosSessionFactory() *PaxosSessionFactory{
  return &PaxosSessionFactory{}
}

func (self *PaxosSessionFactory) Create(conn net.Conn) network.Session {
  return newPaxosSession(conn)
}

type PaxosSession struct {
  conn net.Conn
}

func newPaxosSession(conn net.Conn) *PaxosSession {
  return &PaxosSession{
    conn:conn,
  }
}

func (self *PaxosSession) Handle() {

}
