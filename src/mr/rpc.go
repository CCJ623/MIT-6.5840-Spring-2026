package mr

import (
	"fmt"
	"time"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

type TaskState int

const (
	StateIdle = iota
	StateInProgress
	StateDone
)

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerID_ int
}

type GetTaskArgs struct {
	WorkerID_ int
}

type GetTaskReply struct {
	Type_ TaskType
	TaskID_ int
	FileName_ string
	NumSlices_ int
}

type ReportTaskDoneArgs struct {
	WorkerID_ int
	Type_ TaskType
	TaskID_ int
}

type ReportTaskDoneReply struct {
	OK_ bool
}

/*
intermediate temp filename: mr-intermediate-temp-{MapID}-{ReduceID}-{WorkerID}
intermediate filename: mr-intermediate-{MapID}-{ReduceID}
output temp filename: mr-out-temp-{ReduceID}-{WorkerID}
output filename: mr-out-{ReduceID}
*/

const INTERMEDIATE_FILENAME_PREFIX = "mr-intermediate"
const INTERMEDIATE_TEMP_FILENAME_PREFIX = "mr-intermediate-temp"
const OUTPUT_TEMP_FILENAME_PREFIX = "mr-out-temp"
const OUTPUT_FILENAME_PREFIX = "mr-out"
const TIMEOUT = 10 * time.Second

const DEBUG = false

func DPrintf(format string, a ...interface{}) {
	if DEBUG {
		println(fmt.Sprintf(format, a...))
	}
}
