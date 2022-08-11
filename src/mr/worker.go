package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.Schedule", &args, &reply)
		if !ok {
			log.Fatalf("call Coordinator.Schedule failed")
			break
		}
		exit_reply := WorkerReply{}
		call("Coordinator.RequestExit", &args, &exit_reply)
		if exit_reply.Exit == true {
			break
		}
		if reply.TaskType == MapTaskType {
			intermediate := []KeyValue{}
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("67 line cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			intermediate = append(intermediate, kva...)

			// hash to reduce
			nReduce := reply.NReduce
			buckets := make([][]KeyValue, nReduce)
			var hash_idx int
			for _, kv := range intermediate {
				hash_idx = ihash(kv.Key) % nReduce
				buckets[hash_idx] = append(buckets[hash_idx], kv)
			}

			// write into intermediate files
			for i := range buckets {
				fname := "mr-" + strconv.Itoa(reply.MapTaskIndex) + "-" + strconv.Itoa(i)
				tmpf, err := ioutil.TempFile("", fname+"*")
				if err != nil {
					log.Fatalf("cannot creat temp file: %v", fname)
				}
				enc := json.NewEncoder(tmpf)
				for _, kv := range buckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write to: %v", fname)
					}
				}
				os.Rename(tmpf.Name(), fname)
				tmpf.Close()
			}

			mapArgs := WorkerArgs{reply.MapTaskIndex, -1}
			mapReply := WorkerReply{}
			ok := call("Coordinator.ReceiveDoneMap", &mapArgs, &mapReply)
			if !ok {
				log.Fatalf("call Coordinator.ReceiveDoneMap failed")
			}
		} else if reply.TaskType == ReduceTaskType {
			reduceIdx := reply.ReduceTaskIndex
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIdx)
				// read data from maped file
				tmpf, err := os.Open(fname)
				if err != nil {
					log.Fatalf("114 line cannot read the: %v", fname)
				}
				dec := json.NewDecoder(tmpf)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				tmpf.Close()
			}
			// sort the intermediate of key
			sort.Sort(ByKey(intermediate))

			// distinct key append values (slide window)
			oname := "mr-out-" + strconv.Itoa(reduceIdx)
			tmpf, err := ioutil.TempFile("", oname+"*")
			if err != nil {
				log.Fatalf("cannot creat temp file: %v", tmpf.Name())
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(tmpf, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(tmpf.Name(), oname)
			tmpf.Close()

			for i := 0; i < reply.NMap; i++ {
				fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIdx)
				// read data from maped file
				err := os.Remove(fname)
				if err != nil {
					log.Fatalf("cannot remove the: %v", fname)
				}
			}

			reduceArgs := WorkerArgs{-1, reply.ReduceTaskIndex}
			reduceReply := WorkerReply{}
			ok := call("Coordinator.ReceiveDoneReduce", &reduceArgs, &reduceReply)
			if !ok {
				log.Fatalf("call Coordinator.ReceiveDoneReduce failed")
			}
		} else {
			time.Sleep(time.Second)
		}
	}
	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
