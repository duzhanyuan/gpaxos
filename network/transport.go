package network

import "net"

type Session interface {
  Handle()
  Stop()
}

type SessionFactory interface {
  Create(conn net.Conn) Session
}

type Transport interface {
  Close()
  SendMessage(sendNodeId uint64, msg []byte) error
  BroadcastMessage(msg []byte) error
}
