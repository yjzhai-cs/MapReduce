package mr

import "6.5840/utils"
import "log"
import "os"
import "fmt"
import "path"
import "sync"
import "strconv"
import "encoding/json"
import "time"
import "strings"

type Coordinator struct {
	files []string // slice of filename, taskid-filename
	workers []string // workerid, address(the identity of the worker)
	workerState map[string]string // {address: state(idle, in-progress, completed)}
	location map[string]string // {address: location}
	workerTaskList map[string]map[string]bool // {address: {map/reduce-taskid: true/false}}

	// There are M map tasks and R reduce tasks to assign
	nMap int // the number of map
	nReduce int // the number of reduce
	minWorker int

	mux sync.Mutex // lock
	wgForMapTask sync.WaitGroup
	wgForReduceTask sync.WaitGroup
	wgForCompletion sync.WaitGroup
}


func (c *Coordinator) ReceiveConnectHandler(args *Args, reply *Reply) error {
	address := args.Data["ip"] + ":" + args.Data["port"]
	c.mux.Lock()
	if _, state := c.workerState[address]; !state { // ignore repeated request
		c.workers = append(c.workers, address)
		c.workerState[address] = STATE_IDLE // init state with idle
		if len(c.workers) <= c.minWorker {
			defer c.wgForMapTask.Done()
		} else {
			c.minWorker = -1
		}
		log.Println("Worker[" + address + "] requests to establish connection.")
	}
	c.mux.Unlock()
	return nil
}

func (c *Coordinator) MapTaskCompletedHandler(args *Args, reply *Reply) error {
	address := args.Data["ip"] + ":" + args.Data["port"]
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.workerTaskList[address][fmt.Sprintf("map-%s", args.Data["taskId"])] { // ignore repeated requests
		log.Printf("Worker[%s] send repeated request!\n", address)
		return nil
	}
	c.workerTaskList[address][fmt.Sprintf("map-%s", args.Data["taskId"])] = true // update task state
	idle := true
	for taskid, state := range c.workerTaskList[address] {
		if strings.Contains(taskid, "map") && !state {
			idle = false
			break
		}
	}
	if idle { c.workerState[address] = STATE_IDLE } // update the worker's state
	c.location[address] = args.Data["fileLocation"] // save the intermediate location from worker
	log.Printf("Worker[%s] has completed map task whose taskid is %s. \n", address, args.Data["taskId"])
	defer c.wgForReduceTask.Done()
	return nil
}

func (c *Coordinator) ReduceTaskCompletedHandler(args *Args, reply *Reply) error {
	address := args.Data["ip"] + ":" + args.Data["port"]
	c.mux.Lock()
	if c.workerTaskList[address][fmt.Sprintf("reduce-%s", args.Data["taskId"])] {
		log.Printf("Worker[%s] send repeated request!\n", address) // ignore repeated requests
		c.mux.Unlock()
		return nil
	}
	c.workerTaskList[address][fmt.Sprintf("reduce-%s", args.Data["taskId"])] = true // update task state
	completed := true
	for taskid, state := range c.workerTaskList[address] {
		if strings.Contains(taskid, "reduce") && !state {
			completed = false
			break
		}
	}
	if completed { c.workerState[address] = STATE_COMPLETED } // update the worker's state
	c.mux.Unlock()
	output := utils.MapUnmarshal(args.Data["output"]) // deserialize
	utils.SaveMapToFile(".", "mr-out-" + args.Data["taskId"], ".txt", output) // save the output
	log.Printf("Worker[%s] has completed reduce task whose taskid is %s.\n", address,  args.Data["taskId"])
	// c.wgForCompletion.Done() // If some things cause the goroutine to fail without causing the program to fail, c.wgForReduceTask.Done will never be executed. So the parent-goroutine will wait forever.
	defer c.wgForCompletion.Done()
	return nil
}

var _ MapReduceCoordinatorService = (*Coordinator)(nil) // check if Coordinator implements the interface

func (c *Coordinator) netSocketServer() {
	PRCServer("tcp", ":1234", "Coordinator", c)
	c.monitor() // monitor on backgroubd
	go func(c *Coordinator) { c.assignMapTask()}(c) // assignMapTask is executed only once
	go func(c *Coordinator) { c.assignReduceTask()}(c)
}

func (c *Coordinator) monitor() {
	go func(c *Coordinator) {
		for {
			time.Sleep(2000 * time.Millisecond) // sleep 1000ms
			c.checkWorkerAlive()
		}
	}(c)
}

//  Assign Alogorithm.
//  The coordinator picks idle workers and assigns each one a map task.
//  At least one worker survived.
//  A worker may execute multiple map tasks.
//  'return false' means that all workers are dead.
func (c *Coordinator) assignMapTask() bool {
	c.wgForMapTask.Wait()
	// time.Sleep(5000*time.Millisecond)
	dir, _ := os.Getwd() // get the dir 
	c.mux.Lock()
	defer c.mux.Unlock()
	for taskid, filename := range c.files {
		args := Args {
			Request: REQUEST_MAP,
			Data: map[string]string {
				"filepath": path.Join(dir, filename), "filename": filename,
				"mapTaskId": strconv.Itoa(taskid), "nReduce": strconv.Itoa(c.nReduce),
			},}
		step, id := 0, taskid % len(c.workers)
		for c.workerState[c.workers[id]] == STATE_FAILED || 
		    !RPCCall(CLIENT, MAP_TASK_HANDLER, &args, &Reply{}, c.workers[id], false) { // faul-tolerant
			step, id = step + 1, id + 1 
			if id >= len(c.workers) { id = 0 }
			if step >= len(c.workers) { return false }
		}
		addr := c.workers[id]
		c.workerState[addr] = STATE_IN_PROGRESS // update the state
		if c.workerTaskList[addr] == nil { c.workerTaskList[addr] = make(map[string]bool) }
		c.workerTaskList[addr][fmt.Sprintf("map-%d", taskid)] = false // record the map{worker, taskid}
	}
	log.Println("[Server] assigns map task.")
	return true// destroy goroutinue
}

// 
// The coordinator picks idle workers and assigns each one a reduce task
// 
func (c *Coordinator) assignReduceTask() bool {
	c.wgForReduceTask.Wait()
	c.mux.Lock()
	defer c.mux.Unlock()
	str, err := json.Marshal(c.location)
	if err != nil { log.Fatal(err)}
	for taskid := 0; taskid < c.nReduce; taskid ++ {
		args := Args {
			Request: REQUEST_REDUCE,
			Data: map[string]string {
				"reduceTaskId": strconv.Itoa(taskid), "location": string(str),
			},}
		step, id := 0, taskid % len(c.workers)
		for c.workerState[c.workers[id]] == STATE_FAILED || 
		    !RPCCall(CLIENT, REDUCE_TASK_HANDLER, &args, &Reply{}, c.workers[id], false) { // faul-tolerant
			step, id = step + 1, id + 1
			if id >= len(c.workers) { id = 0 }
			if step >= len(c.workers) { return false }
		}
		addr := c.workers[id]
		c.workerState[addr] = STATE_IN_PROGRESS // update worker state
		if c.workerTaskList[addr] == nil { c.workerTaskList[addr] = make(map[string]bool) }
		c.workerTaskList[addr][fmt.Sprintf("reduce-%d", taskid)] = false // record the map{worker, taskid}
		log.Printf("[Server] assigns worker[%s]-task[%d]\n", c.workers[id],taskid)
	}
	log.Println("[Server] assigns reduce task.")
	return true// destroy goroutinue
}

func (c *Coordinator) checkWorkerAlive() {
	c.mux.Lock()
	backup := utils.Copy(c.workerState).(map[string]string)
	c.mux.Unlock()
	for addr, state := range backup {
		if state == STATE_FAILED { continue }
		args := Args {
			Request: REQUEST_CHECK_WORKER_ALIVE,
			Data: nil,
		}
		if !RPCCall(CLIENT, RECEIVE_CHECK_ALIVE_HANDLER, &args, &Reply{}, addr, false) {
			c.mux.Lock()
			c.workerState[addr] = STATE_FAILED
			c.mux.Unlock()
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.wgForCompletion.Wait()
	for {
		time.Sleep(30000)
	}
	c.mux.Lock()
	for _, addr := range c.workers {
		if c.workerState[addr] == STATE_FAILED { continue }
		args := Args {
			Request:REQUEST_COMPLETED_JOB,
			Data: nil,
		}
		RPCCall(CLIENT, RECEIVE_JOB_FINISHED_HANDLER, &args, &Reply{}, addr, false) // faul-tolerant
	}
	c.mux.Unlock()
	log.Println("[Server] MapReduce job is Finished!")
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{  // create a Coordinator
		files:files,
		workers:make([]string, 0),
		location:make(map[string]string),
		workerState:make(map[string]string),
		workerTaskList:make(map[string]map[string]bool),
		nMap: NUM_MAP, nReduce:nReduce, minWorker: MIN_NUM_WORKER,
	}
	c.wgForMapTask.Add(MIN_NUM_WORKER)
	c.wgForReduceTask.Add(NUM_MAP)
	c.wgForCompletion.Add(nReduce)
	c.netSocketServer() // start tcp server
	return &c
}
