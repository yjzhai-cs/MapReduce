package mr

import "6.5840/utils"
import "log"
import "hash/fnv"
import "encoding/json"
import "path"
import "os"
import "strconv"
import "sort"
import "sync"
// import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Partition struct {
	kva []KeyValue
}

const INTERMEDIATE string = "intermediate"
const JSON_POSTFIX string = ".json"

type Client struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
	ip string
	port string
	mapTaskId []string // list of map-task-id
	reduceTaskId []string // list of reduce-task-id
	channel chan map[string]interface{}
	mux sync.Mutex
}


var _ MapReduceWorkerService = (*Client)(nil) // check if Client implements the interface


// A worker who is assigned a map task reads the contents of the
// contents of the corresponding input split.
// execute map task
func (c *Client) MapTaskHandler(args *Args, reply *Reply) error {
	log.Printf("[%s:%s]Receive the map task!\n", c.ip, c.port)
	// time.Sleep(5000 * time.Millisecond)
	if content, err := utils.ReadFromTxt(args.Data["filepath"]); err == nil {
		kva := c.mapf(args.Data["filename"], content) // execute the map operation and the intermediate key/value pairs are buffered in memory
		if nReduce, err := strconv.Atoi(args.Data["nReduce"]); err == nil { // split kva into c.NReduce pieces
			partitions := partition(kva, nReduce)
			for i, part := range(partitions) {
				filename := "mr-" + args.Data["mapTaskId"] + "-" + strconv.Itoa(i) + JSON_POSTFIX
				intermediate(filename, part.kva) // the buffered pairs are written to local disk,periodically 
			}
		} else {
			log.Fatal("Can't transform string into int. Error:", err)
		}
	} else {
		log.Fatalf("Can't read %v", args.Data["filename"])
	}
	c.mux.Lock()
	c.mapTaskId = append(c.mapTaskId, args.Data["mapTaskId"])
	c.mux.Unlock()
	c.channel <- map[string]interface{}{
		"request":REQUEST_COMPLETED_MAP_TASK, "taskid": args.Data["mapTaskId"],
	}
	return nil
}

// When a reduce worker is notified by the coordinator about the locations,
// it uses RPC to read the buffered data from the local disk of the map workers.
// execute reduce task.
func (c *Client) ReduceTaskHandler(args *Args, reply *Reply) error {
	log.Printf("[%s:%s]Receive the reduce task!\n", c.ip, c.port)
	// time.Sleep(5000 * time.Millisecond)
	reduceTaskId := args.Data["reduceTaskId"]
	location := utils.MapUnmarshal(args.Data["location"]) // deserialize
	kva := c.requestIntermediate(reduceTaskId, location) // get the inermedidate from other workers(file server)
	sort.Slice(kva, func (i int, j int) bool { // sort kva by Key so that the same key is grouped together 
		return kva[i].Key < kva[j].Key
	})
	res := make(map[string]string) // save the reduce output
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j ++
		}
		values := []string{}
		for k := i; k < j; k ++ {
			values = append(values, kva[k].Value)
		}
		res[kva[i].Key] = c.reducef(kva[i].Key, values)
		i = j
	}
	c.mux.Lock()
	c.reduceTaskId = append(c.reduceTaskId, reduceTaskId)
	c.mux.Unlock()
	c.channel <- map[string]interface{} {
		"request":REQUEST_COMPLETED_REDUCE_TASK, "taskid":reduceTaskId, "res":res,
	}
	return nil
}

func (c *Client) ReturnIntermediateHandler(args *Args, reply *Reply) error {
	kva := make([]KeyValue, 0)
	for _, maptaskid := range c.mapTaskId {
		filename := "mr-" + maptaskid + "-" + args.Data["reduceTaskId"] + JSON_POSTFIX
		temp := dejsonize(path.Join(args.Data["location"], filename)) // read data([] KeyValue) from json file
		kva = append(kva, temp...)
	}
	if str, err := json.Marshal(kva); err != nil {
		log.Fatal("Serialization failed! Error:", err)
		return nil
	} else {
		reply.Data = map[string]string { "intermediate":string(str), }
	}
	log.Printf("Receive task of reading intermediate file(reduceTaskId=%s)!\n", args.Data["reduceTaskId"])
	return nil
}

func (c *Client) ReceiveCheckAliveHandler(args *Args, reply *Reply) error {
	return nil
}


func (c *Client) ReceiveJobFinishedHandler(args *Args, reply *Reply) error {
	c.channel <- map[string]interface{} {
		"request":REQUEST_COMPLETED_JOB,
	}
	return nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// split kva into c.NReduce pieces
func partition(kva []KeyValue, NReduce int) []Partition {
	backup := make([]KeyValue, len(kva))
	copy(backup, kva) // get the copy
	sort.Slice(backup, func (i int, j int) bool { // sort by Key
		return backup[i].Key < backup[j].Key
	})
	partitions := make([]Partition, NReduce)
	for _, kv := range backup {
		pos := ihash(kv.Key) % NReduce
		if partitions[pos].kva == nil {
			partitions[pos].kva = make([]KeyValue, 0)
		}
		partitions[pos].kva = append(partitions[pos].kva, kv)
	}
	return partitions
}

// save the map result as intermediate such as map-X-Y
func intermediate(filename string, kva []KeyValue) {
	dir, _ := os.Getwd()
	filePath := path.Join(dir, "../", INTERMEDIATE , filename)
	file, err := os.Create(filePath)
	defer file.Close()
	if err != nil {
		log.Fatalf("Can't create the %v. Error: %v", filename, err)
		return
	}
	enc := json.NewEncoder(file)
	for _, kv := range kva { // kv is a copy
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("Can't write. Error: %v", err)
			break
		}
	}
}

func dejsonize(filePath string) []KeyValue {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		log.Fatal("Can't open file. Error:", err)
		return nil
	}
	kva := make([]KeyValue, 0)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func (c *Client) dispatcher() {
	for {
		value := <- c.channel
		if value["request"] == REQUEST_COMPLETED_MAP_TASK {
			c.requestCompleteMapTask(MAP_TASK, value["taskid"].(string))
			log.Println("Complete map task! Wait for new task from coordinator.")
		} else if value["request"] == REQUEST_COMPLETED_REDUCE_TASK {
			c.requestCompleteReduceTask(REDUCE_TASK, value["taskid"].(string),
			                             value["res"].(map[string]string))
			log.Println("Complete reduce task! Wait for new task from coordinator.")
		} else if value["request"] == REQUEST_COMPLETED_JOB {
			log.Println("MapReduce job is Finished!")
			return
		}
	}
}

func (c *Client) requestConnect() bool {
	args := Args {
		Request: REQUEST_CONNECT,
		Data: map[string]string{
			"ip": c.ip,
			"port": c.port,
		},
	}
	return RPCCall(COORDINATOR, RECEIVE_CONNECT_HANDLER ,&args, &Reply{}, 
		    SERVER_IP + ":" + SERVER_PORT, false)
}

// 
// the locations of the buffered pairs on the local disk are passed back to the coordinator.
// 
func (c *Client) requestCompleteMapTask(taskType string, taskid string) bool {
	dir, _ := os.Getwd()
	filePath := path.Join(dir, "../", INTERMEDIATE)
	args := Args {
		Request: REQUEST_COMPLETED_MAP_TASK,
		Data: map[string]string{
			"ip": c.ip,
			"port": c.port,
			"taskType": taskType,
			"taskId": taskid,
			"fileLocation": filePath, 
 		},
	}
	return RPCCall(COORDINATOR, MAP_TASK_COMPLETED_HANDLER, &args, &Reply{}, 
		     SERVER_IP + ":" + SERVER_PORT, false)
}

func (c *Client) requestCompleteReduceTask(taskType string, taskid string, res map[string]string) bool {
	args := Args {
		Request: REQUEST_COMPLETED_REDUCE_TASK,
		Data: map[string]string {
			"ip": c.ip,
			"port": c.port,
			"taskType": taskType,
			"taskId": taskid,
			"output": utils.MapMarshal(res),
		},
	}
	return RPCCall(COORDINATOR, REDUCE_TASK_COMPLETED_HANDLER, &args, &Reply{}, 
		SERVER_IP + ":" + SERVER_PORT, false)
}

func (c *Client) requestIntermediate(reduceTaskId string, location map[string]string) []KeyValue {
	kva := make([]KeyValue, 0)
	for addr, filePath := range location {
		args := Args {
			Request: REQUEST_INTERMEDIATE,
			Data: map[string]string {
				"reduceTaskId": reduceTaskId,
				"location": filePath,
			},
		}
		reply := Reply {}
		RPCCall(CLIENT, RETURN_INTERMEDIATE_HANDLER, &args, &reply, addr, true)
		var temp []KeyValue
		if err := json.Unmarshal([]byte(reply.Data["intermediate"]), &temp); err != nil {
			log.Fatal("Unmarshal failed. Error", err)
		}
		kva = append(kva, temp...)
	}
	return kva
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	port, err := utils.GetFreePort()
	if err != nil {
		log.Fatal("Can't get free port. Error:", err)
		return
	}
	client := Client{
		mapf:mapf,  reducef:reducef,
		ip:"localhost", port: strconv.Itoa(port),
		mapTaskId:make([]string, 0), reduceTaskId:make([]string, 0),
		channel: make(chan map[string]interface{}),
	}
	PRCServer("tcp", ":" + strconv.Itoa(port), "Client", &client) // listen server
	if ok := client.requestConnect(); !ok { // connect server
		return 
	}
	log.Println("Client[" + client.ip + ":" + client.port + "]" + " connects to coordinator successfully and waits for task from coordinator.")
	// go client.consumer()
	client.dispatcher() // dispatch request
}
