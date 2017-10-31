package network

type Transport interface {
  SendMessage(sendNodeId uint64, msg []byte) error
  BroadcastMessage(msg []byte) error
}
