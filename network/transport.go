package network

import "net"

type Session interface {
  Handle()
}

type SessionFactory interface {
  Create(conn net.Conn) Session
}

type Transport interface {
  SendMessage(sendNodeId uint64, msg []byte) error
  BroadcastMessage(msg []byte) error
}
