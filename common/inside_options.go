package common

type InsideOptions struct {
  logFileMaxSize int64

  prepareTimeousMs uint32
  maxPrepareTimeoutMs uint32

  acceptTimeousMs uint32
  maxAcceptTimeoutsMs uint32

  maxCommitTimeoutMs uint32
}

var gInsideOptions InsideOptions

func init() {
  gInsideOptions.logFileMaxSize = 1024 * 1024 * 100

  gInsideOptions.prepareTimeousMs = 2000
  gInsideOptions.maxPrepareTimeoutMs = 2000
  gInsideOptions.acceptTimeousMs = 1000
  gInsideOptions.maxAcceptTimeoutsMs = 2000

  gInsideOptions.maxCommitTimeoutMs = 5000
}

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
  return gInsideOptions.logFileMaxSize
}

func SetLogFileMaxSize(size int64) {
  gInsideOptions.logFileMaxSize = size
}

func GetStartPrepareTimeoutMs() uint32 {
  return  gInsideOptions.prepareTimeousMs
}

func GetStartAcceptTimeoutMs() uint32 {
  return  gInsideOptions.acceptTimeousMs
}

func GetMaxPrepareTimeoutMs() uint32 {
  return  gInsideOptions.maxPrepareTimeoutMs
}

func GetMaxAcceptTimeoutMs() uint32 {
  return  gInsideOptions.maxAcceptTimeoutsMs
}

func GetMaxCommitTimeoutMs() uint32 {
	return gInsideOptions.maxCommitTimeoutMs
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

func GetAskforLearnInterval() uint32 {
  return 1000
}

func GetLeanerReceiver_Ack_Lead() uint64 {
  return 100
}

func GetMaxValueSize() int {
  return 10240
}