package common

type MsgTransport interface {
    SendMessage(sendToNodeId uint64, buffer []byte, sendType int) error
    BroadcastMessage(buffer []byte, sendType int) error
    BroadcastMessageFollower(buffer []byte, sendType int) error
    BroadcastMessageTempNode(buffer []byte, sendType int) error
}
