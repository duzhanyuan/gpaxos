package common

type MsgTransport interface {
    SendMessage(sendToNodeId uint64, buffer []byte, sendType int32) error
    BroadcastMessage(buffer []byte, sendType int32) error
    BroadcastMessageFollower(buffer []byte, sendType int32) error
    BroadcastMessageTempNode(buffer []byte, sendType int32) error
}
