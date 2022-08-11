package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task_Type int

const (
	MapTaskType     Task_Type = 0
	ReduceTaskType  Task_Type = 1
	DoneType        Task_Type = 2
	WaitingTaskType Task_Type = 3
)

type WorkerArgs struct {
	MapDoneIndex    int // unsure Index
	ReduceDoneIndex int
}

type WorkerReply struct {
	TaskType Task_Type // map task:0, reduce task:1, waiting:2, job Done:3
	NMap     int
	NReduce  int

	MapTaskIndex    int
	ReduceTaskIndex int
	FileName        string
	Exit            bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
