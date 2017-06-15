package common

const (
  Message_SendType_UDP = 0
  Message_SendType_TCP = 1
)

type MsgTransport interface {
  SendMessage(sendToNodeId uint64, buffer []byte, sendType int) error
  BroadcastMessage(buffer []byte, sendType int) error
  BroadcastMessageFollower(buffer []byte, sendType int) error
  BroadcastMessageTempNode(buffer []byte, sendType int) error
}
