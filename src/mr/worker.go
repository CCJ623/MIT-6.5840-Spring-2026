package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// register
	args := RegisterArgs{}
	reply := RegisterReply{}
	worker_id := -1
	continuous_connect_failed_times := 0

	// if return true, worker should exit
	connect_failed := func() bool {
		continuous_connect_failed_times++
		DPrintf("[Worker] connect failed %v time(s)\n", continuous_connect_failed_times)
		if continuous_connect_failed_times == 3 {
			DPrintf("[Worker] continuous connet failed %v times, exit!\n", continuous_connect_failed_times)
			return true
		}
		return false
	}

	for {
		DPrintf("[Worker] try register")
		ok := call("Coordinator.Register", &args, &reply)
		if ok {
			continuous_connect_failed_times = 0
			if reply.WorkerID_ < 0 {
				DPrintf("[Worker] register failed")
				continue
			}
			worker_id = reply.WorkerID_
			DPrintf("[Worker %v] register success\n", worker_id)
			break
		} else {
			DPrintf("[Worker] register rpc failed")
			if connect_failed() {
				return
			}
		}
	}

	// work
	for {
		// try get task
		args := GetTaskArgs{}
		args.WorkerID_ = worker_id
		reply := GetTaskReply{}
		DPrintf("[Worker %v] ask coordinator for task\n", worker_id)

		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			DPrintf("[Worker %v] get task rpc failed\n", worker_id)
			if connect_failed() {
				return
			}
			continue
		}

		continuous_connect_failed_times = 0
		task_id := reply.TaskID_
		switch reply.Type_ {

		case TaskExit:
			DPrintf("[Worker %v] receive task: exit\n", worker_id)
			return

		case TaskWait:
			DPrintf("[Worker %v] receive task: wait\n", worker_id)
			time.Sleep(time.Second)
			continue

		case TaskMap:
			DPrintf("[Worker %v] receive map task[%v] file=%s nReduce=%v\n", worker_id, task_id, reply.FileName_, reply.NumSlices_)
			filename := reply.FileName_
			intermediates := make([][]KeyValue, reply.NumSlices_)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			DPrintf("[Worker %v] map task[%v] produced %v key/value pairs\n", worker_id, task_id, len(kva))

			// partition
			for _, kv := range kva {
				index := ihash(kv.Key) % reply.NumSlices_
				intermediates[index] = append(intermediates[index], kv)
			}
			DPrintf("[Worker %v] map task[%v] partition finished\n", worker_id, task_id)

			// sort and save file
			for index := range intermediates {
				sort.Sort(ByKey(intermediates[index]))

				// save temp intermediate file
				temp_filename := fmt.Sprintf("%s-%v-%v-%v", INTERMEDIATE_TEMP_FILENAME_PREFIX, reply.TaskID_, index, worker_id)
				ofile, _ := os.Create(temp_filename)
				for _, kv := range intermediates[index] {
					fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				}
				ofile.Close()
				finalFilename := fmt.Sprintf("%s-%v-%v", INTERMEDIATE_FILENAME_PREFIX, reply.TaskID_, index)
				DPrintf("[Worker %v] map task[%v] write %s (%v records)\n", worker_id, task_id, temp_filename, len(intermediates[index]))

				// atomic rename intermediate file
				os.Rename(temp_filename, finalFilename)
				DPrintf("[Worker %v] map task[%v] rename %s -> %s\n", worker_id, task_id, temp_filename, finalFilename)
			}

			// report done
			args := ReportTaskDoneArgs{}
			args.Type_ = TaskMap
			args.TaskID_ = task_id
			args.WorkerID_ = worker_id
			reply := ReportTaskDoneReply{}
			DPrintf("[Worker %v] report map task[%v] done\n", worker_id, task_id)
			ok := call("Coordinator.ReportTaskDone", &args, &reply)
			if !ok {
				DPrintf("[Worker %v] report task[%v] done rpc failed\n", worker_id, task_id)
				if connect_failed() {
					return
				}
				continue
			}

			continuous_connect_failed_times = 0
			DPrintf("[Worker %v] coordinator acknowledged map task[%v] done\n", worker_id, task_id)
			// done, try to get new task

		case TaskReduce:
			DPrintf("[Worker %v] receive reduce task[%v] nMap=%v\n", worker_id, task_id, reply.NumSlices_)

			// read all intermediate to kvs
			kvs := make([]KeyValue, 0)
			for index := 0; index < reply.NumSlices_; index++ {

				// read file
				filename := fmt.Sprintf("%s-%v-%v", INTERMEDIATE_FILENAME_PREFIX, index, task_id)
				DPrintf("[Worker %v] reduce task[%v] reading intermediate file %s\n", worker_id, task_id, filename)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()

				// parse file and add kv to kvs
				for _, kv_string := range strings.FieldsFunc(string(content), func(c rune) bool {
					return c == '\n' || c == '\r'
				}) {
					kv_slice := strings.Fields(kv_string)
					kvs = append(kvs, KeyValue{kv_slice[0], kv_slice[1]})
				}
			}
			sort.Sort(ByKey(kvs))

			// do reduce
			keys := make([]string, 0)
			result := make(map[string]string)
			values := make([]string, 0)
			for _,kv := range kvs{
				values =append(values, kv.Value)
			}

			for i := 0; i < len(kvs); {
				j := i + 1
				for ; j < len(kvs); j++ {
					if kvs[i].Key == kvs[j].Key {
						continue
					} else {
						key := kvs[i].Key
						keys = append(keys, key)
						result[key] = reducef(key, values[i:j])

						i = j
						break
					}
				}

				// do reduce of last piece
				if j >= len(kvs) {
					key := kvs[i].Key
					keys = append(keys, key)
					result[key] = reducef(key, values[i:j])
					break
				}
			}

			// write to temp output file
			temp_output_filename := fmt.Sprintf("%s-%v-%v", OUTPUT_TEMP_FILENAME_PREFIX, reply.TaskID_, worker_id)
			ofile, _ := os.Create(temp_output_filename)
			for _, key := range keys {
				fmt.Fprintf(ofile, "%v %v\n", key, result[key])
			}
			ofile.Close()

			// atomic rename output file
			output_filename := fmt.Sprintf("%s-%v", OUTPUT_FILENAME_PREFIX, reply.TaskID_)
			os.Rename(temp_output_filename, output_filename)
			DPrintf("[Worker %v] reduce task[%v] rename %s -> %s\n", worker_id, task_id, temp_output_filename, output_filename)

			// report done
			args := ReportTaskDoneArgs{}
			args.Type_ = TaskReduce
			args.TaskID_ = task_id
			args.WorkerID_ = worker_id
			reply := ReportTaskDoneReply{}
			DPrintf("[Worker %v] report reduce task[%v] done\n", worker_id, task_id)
			ok := call("Coordinator.ReportTaskDone", &args, &reply)
			if !ok {
				DPrintf("[Worker %v] report task[%v] done rpc failed\n", worker_id, task_id)
				if connect_failed() {
					return
				}
				continue
			}

			continuous_connect_failed_times = 0
			DPrintf("[Worker %v] coordinator acknowledged reduce task[%v] done\n", worker_id, task_id)
			// done, try to get new task

		default:
			DPrintf("[Worker %v] receive wrong task type=%v\n", worker_id, reply.Type_)
			continue
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		DPrintf("reply.Y %v\n", reply.Y)
	} else {
		DPrintf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
