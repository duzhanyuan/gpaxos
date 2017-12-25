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
	PaxosTryCommitRet_WaitTimeout                 = errors.New("PaxosTryCommitRet_WaitTimeout")
  PaxosTryCommitRet_TooManyThreadWaiting_Reject = errors.New("PaxosTryCommitRet_TooManyThreadWaiting_Reject")
)

var (
  Paxos_SystemError                           = errors.New("Paxos_SystemError")
  Paxos_GroupIdxWrong                         = errors.New("Paxos_GroupIdxWrong")
  Paxos_MembershipOp_GidNotSame               = errors.New("Paxos_MembershipOp_GidNotSame")
  Paxos_MembershipOp_VersionConflit           = errors.New("Paxos_MembershipOp_VersionConflit")
  Paxos_MembershipOp_NoGid                    = errors.New("Paxos_MembershipOp_NoGid")
  Paxos_MembershipOp_Add_NodeExist            = errors.New("Paxos_MembershipOp_Add_NodeExist")
  Paxos_MembershipOp_Remove_NodeNotExist      = errors.New("Paxos_MembershipOp_Remove_NodeNotExist")
  Paxos_MembershipOp_Change_NoChange          = errors.New("Paxos_MembershipOp_Change_NoChange")
  Paxos_GetInstanceValue_Value_NotExist       = errors.New("Paxos_GetInstanceValue_Value_NotExist")
  Paxos_GetInstanceValue_Value_Not_Chosen_Yet = errors.New("Paxos_GetInstanceValue_Value_Not_Chosen_Yet")
)

const (
  SYSTEM_V_SMID      = 100000000
  MASTER_V_SMID      = 100000001
  BATCH_PROPOSE_SMID = 100000002
)
