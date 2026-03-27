package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	State_    TaskState
	WorkerID_ int
	Start_    time.Time
}

type Coordinator struct {
	// Your definitions here.
	lock_ sync.Mutex

	files_      []string
	num_map_    int
	num_reduce_ int
	phase_      TaskType
	tasks_      []TaskInfo
	// next worker id
	worker_register_ int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (coordinator *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	coordinator.lock_.Lock()
	defer coordinator.lock_.Unlock()

	if coordinator.phase_ == TaskExit {
		reply.WorkerID_ = -1
		return nil
	}

	if coordinator.worker_register_ < 0 {
		reply.WorkerID_ = -1
		return nil
	}

	reply.WorkerID_ = coordinator.worker_register_
	coordinator.worker_register_++
	DPrintf("[Coordinator] Register Worker[%v]\n", reply.WorkerID_)
	return nil
}

func (coordinator *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	coordinator.lock_.Lock()
	defer coordinator.lock_.Unlock()

	switch coordinator.phase_ {

	case TaskMap:
		for index, filename := range coordinator.files_ {
			task := &coordinator.tasks_[index]
			switch task.State_ {

			case StateIdle:
				task.State_ = StateInProgress
				task.Start_ = time.Now()
				task.WorkerID_ = args.WorkerID_

				reply.FileName_ = filename
				reply.NumSlices_ = coordinator.num_reduce_
				reply.TaskID_ = index
				reply.Type_ = TaskMap

				DPrintf("[Coordinator] Worker[%v] get task[%v]\n", args.WorkerID_, reply.TaskID_)

				return nil

			case StateInProgress:
				if time.Since(task.Start_) > TIMEOUT {
					// reassign timeout task
					task.Start_ = time.Now()
					old_worker_id := task.WorkerID_
					task.WorkerID_ = args.WorkerID_

					reply.FileName_ = filename
					reply.NumSlices_ = coordinator.num_reduce_
					reply.TaskID_ = index
					reply.Type_ = TaskMap

					DPrintf("[Coordinator] Worker[%v] get task[%v](Worker[%v] timeout)\n", args.WorkerID_, reply.TaskID_, old_worker_id)

					return nil
				}

			case StateDone:
				continue
			default:
			}
		}
		// no available task
		reply.Type_ = TaskWait
		DPrintf("[Coordinator] Worker[%v] should wait(out of task)\n", args.WorkerID_)

		return nil

	case TaskReduce:
		for index := range coordinator.tasks_ {
			task := &coordinator.tasks_[index]

			switch task.State_ {
			case StateIdle:

				task.State_ = StateInProgress
				task.Start_ = time.Now()
				task.WorkerID_ = args.WorkerID_

				reply.NumSlices_ = coordinator.num_map_
				reply.TaskID_ = index
				reply.Type_ = TaskReduce

				DPrintf("[Coordinator] Worker[%v] get task[%v]\n", args.WorkerID_, reply.TaskID_)

				return nil
			case StateInProgress:
				if time.Since(task.Start_) > TIMEOUT {

					task.Start_ = time.Now()
					old_worker_id := task.WorkerID_
					task.WorkerID_ = args.WorkerID_

					reply.NumSlices_ = coordinator.num_map_
					reply.TaskID_ = index
					reply.Type_ = TaskReduce

					DPrintf("[Coordinator] Worker[%v] get task[%v](Worker[%v] timeout)\n", args.WorkerID_, reply.TaskID_, old_worker_id)

					return nil
				}
			case StateDone:
				continue
			default:
			}
		}
		// no available task
		reply.Type_ = TaskWait
		DPrintf("[Coordinator] Worker[%v] should wait(out of task)\n", args.WorkerID_)

		return nil

	case TaskExit:
		reply.Type_ = TaskExit
		DPrintf("[Coordinator] Worker[%v] should exit(mission completed)\n", args.WorkerID_)
		return nil

	default:
		return &net.ParseError{}
	}
}

func (coordinator *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	coordinator.lock_.Lock()
	defer coordinator.lock_.Unlock()

	replyOK := func() error {
		reply.OK_ = true
		return nil
	}

	// skip wrong type
	if args.Type_ != coordinator.phase_ {
		DPrintf("[Coordinator] Worker[%v] ignored(wrong type)\n", args.WorkerID_)
		return replyOK()
	}

	task := &coordinator.tasks_[args.TaskID_]
	// skip duplicate task
	if task.State_ == StateDone {
		DPrintf("[Coordinator] Worker[%v] ignored(duplicate)\n", args.WorkerID_)
		return replyOK()
	}

	DPrintf("[Coordinator] Worker[%v] done task[%v]\n", args.WorkerID_, args.TaskID_)
	task.State_ = StateDone
	task.WorkerID_ = args.WorkerID_

	switch coordinator.phase_ {

	case TaskMap:
		// check every task done
		for index := range coordinator.tasks_ {
			task = &coordinator.tasks_[index]
			if task.State_ != StateDone {
				return replyOK()
			}
		}

		// all task done
		coordinator.phase_ = TaskReduce
		coordinator.tasks_ = make([]TaskInfo, coordinator.num_reduce_)

		for index := range coordinator.tasks_ {
			task = &coordinator.tasks_[index]
			task.State_ = StateIdle
		}
		DPrintf("---------------- Reduce Phase ----------------\n")
		return replyOK()

	case TaskReduce:
		// check if every task done
		for index := range coordinator.tasks_ {
			task = &coordinator.tasks_[index]
			if task.State_ != StateDone {
				return replyOK()
			}
		}

		// all task done
		coordinator.phase_ = TaskExit
		DPrintf("---------------- Exit Phase ----------------\n")
		return replyOK()

	case TaskExit:
		return replyOK()

	default:
		return &net.ParseError{}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock_.Lock()
	defer c.lock_.Unlock()
	ret = (c.phase_ == TaskExit)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files_ = make([]string, len(files))
	c.tasks_ = make([]TaskInfo, len(c.files_))
	for index, filename := range files {
		c.files_[index] = filename
		c.tasks_[index].State_ = StateIdle
	}
	c.num_map_ = len(files)
	c.num_reduce_ = nReduce
	c.phase_ = TaskMap
	c.server(sockname)
	DPrintf("---------------- Map Phase ----------------\n")
	return &c
}
