package gpaxos

type StateMachineContext struct {
  SMId int32
  Context interface{}
}

func NewStateMachineContext(smid int32, context interface{}) *StateMachineContext {
  return &StateMachineContext{
    SMId:smid,
    Context:context,
  }
}

type BatchStateMachineContext struct {
  StateMachineContexts []*StateMachineContext
}

func NewBatchStateMachineContext() *BatchStateMachineContext {
  return &BatchStateMachineContext{
    StateMachineContexts:make([]*StateMachineContext, 0),
  }
}

type StateMachine interface {
  Execute(instanceId uint64, paxosValue[]byte, context *StateMachineContext) error

  SMID() int32

  ExecuteForCheckpoint(instanceId uint64, paxosValue []byte)error

  GetCheckpointInstanceID() uint64

  LockCheckpointState() error

  GetCheckpointState(dirPath *string, fileList []string) error

  UnLockCheckpointState()

  LoadCheckpointState(checkpointTmpFileDirPath string,
    fileList []string, checkpointInstanceID uint64) error
}
