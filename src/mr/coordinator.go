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

type TaskStatus int

const (
	Init    TaskStatus = 0
	Running TaskStatus = 1
	Done    TaskStatus = 2
)

type AtomicLog struct {
	status []TaskStatus
	mux    sync.Mutex
}

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	nMap          int
	files         []string
	mapDones      int
	mapTaskLog    AtomicLog // log for map task, 0: Init, 1: Running, 2:Done
	reduceDones   int
	reduceTasklog AtomicLog // log for reduce task
	mux           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReceiveDoneMap(args *WorkerArgs, reply *WorkerReply) error {
	c.mux.Lock()
	c.mapDones++
	c.mux.Unlock()

	c.mapTaskLog.mux.Lock()
	defer c.mapTaskLog.mux.Unlock()
	c.mapTaskLog.status[args.MapDoneIndex] = Done

	return nil
}

func (c *Coordinator) ReceiveDoneReduce(args *WorkerArgs, reply *WorkerReply) error {
	c.mux.Lock()
	c.reduceDones++
	c.mux.Unlock()

	c.reduceTasklog.mux.Lock()
	defer c.reduceTasklog.mux.Unlock()
	c.reduceTasklog.status[args.ReduceDoneIndex] = Done
	return nil
}

func (c *Coordinator) Schedule(args *WorkerArgs, reply *WorkerReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.mapDones < c.nMap {
		var preMapTaskIdx int
		preMapTaskIdx = -1
		c.mapTaskLog.mux.Lock()
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskLog.status[i] == Init {
				preMapTaskIdx = i
				break
			}
		}
		c.mapTaskLog.mux.Unlock()

		if preMapTaskIdx == -1 {
			reply.TaskType = WaitingTaskType
			return nil
		}

		reply.NReduce = c.nReduce
		reply.MapTaskIndex = preMapTaskIdx
		reply.TaskType = MapTaskType
		reply.FileName = c.files[preMapTaskIdx]
		c.mapTaskLog.mux.Lock()
		c.mapTaskLog.status[preMapTaskIdx] = Running
		c.mapTaskLog.mux.Unlock()

		checkMapDeadline := func(period int) {
			for i := 0; i < period; i++ {
				time.Sleep(time.Second)
			}

			c.mapTaskLog.mux.Lock()
			if c.mapTaskLog.status[preMapTaskIdx] == Running {
				c.mapTaskLog.status[preMapTaskIdx] = Init
			}
			c.mapTaskLog.mux.Unlock()
		}
		go checkMapDeadline(10)
	} else if c.reduceDones < c.nReduce {
		var preReduceTaskIdx int
		preReduceTaskIdx = -1
		c.reduceTasklog.mux.Lock()
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasklog.status[i] == Init {
				preReduceTaskIdx = i
				break
			}
		}
		c.reduceTasklog.mux.Unlock()

		if preReduceTaskIdx == -1 {
			reply.TaskType = WaitingTaskType
			return nil
		}

		reply.NMap = c.nMap
		reply.ReduceTaskIndex = preReduceTaskIdx
		reply.TaskType = ReduceTaskType
		c.reduceTasklog.mux.Lock()
		c.reduceTasklog.status[preReduceTaskIdx] = Running
		c.reduceTasklog.mux.Unlock()
		checkReduceDeadline := func(period int) {
			for i := 0; i < period; i++ {
				time.Sleep(time.Second)
			}

			c.reduceTasklog.mux.Lock()
			if c.reduceTasklog.status[preReduceTaskIdx] == Running {
				c.reduceTasklog.status[preReduceTaskIdx] = Init
			}
			c.reduceTasklog.mux.Unlock()
		}
		go checkReduceDeadline(10)
	} else {
		reply.TaskType = DoneType
	}
	return nil
}

func (c *Coordinator) RequestExit(args *WorkerArgs, reply *WorkerReply) error {
	reply.Exit = c.Done()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	c.mux.Lock()
	defer c.mux.Unlock()
	ret := c.nReduce == c.reduceDones
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTaskLog.status = make([]TaskStatus, c.nMap)
	c.reduceTasklog.status = make([]TaskStatus, c.nReduce)
	c.mapDones = 0
	c.reduceDones = 0
	c.server()
	return &c
}
