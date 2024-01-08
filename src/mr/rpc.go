package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type FinishReducingArg struct {
	Id int
}

type FinishReducingReply struct {
}

type FinishMappingArg struct {
	Id int
}

type FinishMappingReply struct {
}

type Ask4JobArg struct {
}

type Ask4JobReply struct {
	// 1: mapper 2: reducer 3: no job now 4: quit now 0: coordinator missing
	ReplyStat   int
	MapperTask  MapTask
	ReducerTask ReduceTask
	ReduceNum   int
	MapNum      int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
