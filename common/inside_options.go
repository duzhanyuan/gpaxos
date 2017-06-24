package common

type InsideOptions struct {
}

var gInsideOptions *InsideOptions

func (self *InsideOptions) GetLogFileMaxSize() int64 {
  return 10240
}

func (self *InsideOptions) GetMaxIOLoopQueueLen() int32 {
  return 1000
}

func (self *InsideOptions) GetMaxQueueLen() int32 {
  return 1000
}

func GetMaxIOLoopQueueLen() int {
  return 1000
}

func GetMaxQueueLen() int {
  return 1000
}

func GetMaxQueueMemSize() int {
  return 100000
}

func GetMaxRetryQueueLen() int {
  return 10000
}

func GetLogFileMaxSize() int64 {
  return 10240
}

func GetStartPrepareTimeoutMs() uint32 {
  return  1000
}

func GetStartAcceptTimeoutMs() uint32 {
  return 1000
}

func GetMaxPrepareTimeoutMs() uint32 {
  return 5000
}

func GetMaxAcceptTimeoutMs() uint32 {
  return 5000
}

func GetCleanerDeleteQps() uint32 {
  return 1000
}

func GetLeanerSenderPrepareTimeoutMs() uint64 {
  return 1000
}

func GetLeanerSender_Ack_TimeoutMs() uint64 {
  return 1000
}

func GetLeanerSender_Ack_Lead() uint64 {
  return 10
}

func GetLearnerSenderSendQps() uint64 {
  return 2000
}