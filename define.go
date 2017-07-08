package gpaxos

import "errors"

var (
  PaxosTryCommitRet_OK                          = errors.New("PaxosTryCommitRet_OK")
  PaxosTryCommitRet_Reject                      = errors.New("PaxosTryCommitRet_Reject")
  PaxosTryCommitRet_Conflict                    = errors.New("PaxosTryCommitRet_Conflict")
  PaxosTryCommitRet_ExecuteFail                 = errors.New("PaxosTryCommitRet_ExecuteFail")
  PaxosTryCommitRet_Follower_Cannot_Commit      = errors.New("PaxosTryCommitRet_Follower_Cannot_Commit")
  PaxosTryCommitRet_Im_Not_In_Membership        = errors.New("PaxosTryCommitRet_Im_Not_In_Membership")
  PaxosTryCommitRet_Value_Size_TooLarge         = errors.New("PaxosTryCommitRet_Value_Size_TooLarge")
  PaxosTryCommitRet_Timeout                     = errors.New("PaxosTryCommitRet_Timeout")
  PaxosTryCommitRet_TooManyThreadWaiting_Reject = errors.New("PaxosTryCommitRet_TooManyThreadWaiting_Reject")
)

const (
  SYSTEM_V_SMID      = 100000000
  MASTER_V_SMID      = 100000001
  BATCH_PROPOSE_SMID = 100000002
)
