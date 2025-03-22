package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "net"
import "net/rpc"
import "log"
import "fmt"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	Request string
	Data map[string]string
}

type Reply struct {
	Data map[string]string
}

// Add your RPC definitions here.


const (
	REQUEST_CONNECT = "Connect"
	REQUEST_MAP = "Map"
	REQUEST_REDUCE = "Reduce"
	REQUEST_INTERMEDIATE = "Intermediate"
	REQUEST_COMPLETED_JOB = "Completed_Job"
	REQUEST_COMPLETED_MAP_TASK = "Completed_Map"
	REQUEST_COMPLETED_REDUCE_TASK = "Completed_Reduce"
	REQUEST_CHECK_WORKER_ALIVE = "Check_Worker_Alive"
)

const (
	MAP_TASK = "map"
	REDUCE_TASK = "reduce"
)

const NUM_MAP int = 8
const MIN_NUM_WORKER = 2
const (
	SERVER_IP = "localhost"
	SERVER_PORT = "1234"
)
const (
	STATE_IDLE = "idle"
	STATE_IN_PROGRESS = "in-progress"
	STATE_COMPLETED = "completed"
	STATE_FAILED = "failed"
)

const (
	COORDINATOR = "Coordinator"
	MAPREDUCE_HANDLER = "MapReduceHandler"
	RECEIVE_CONNECT_HANDLER = "ReceiveConnectHandler"
	MAP_TASK_COMPLETED_HANDLER = "MapTaskCompletedHandler"
	REDUCE_TASK_COMPLETED_HANDLER = "ReduceTaskCompletedHandler"
)
const (
	CLIENT = "Client"
	MAP_TASK_HANDLER = "MapTaskHandler"
	REDUCE_TASK_HANDLER = "ReduceTaskHandler"
	RETURN_INTERMEDIATE_HANDLER = "ReturnIntermediateHandler"
	RECEIVE_JOB_FINISHED_HANDLER = "ReceiveJobFinishedHandler"
	RECEIVE_CHECK_ALIVE_HANDLER = "ReceiveCheckAliveHandler"
)

type MapReduceCoordinatorService interface {
	// coordinator
	ReceiveConnectHandler(args *Args, reply *Reply) error
	MapTaskCompletedHandler(args *Args, reply *Reply) error
	ReduceTaskCompletedHandler(args *Args, reply *Reply) error
}

type MapReduceWorkerService interface {
	// worker
	MapTaskHandler(args *Args, reply *Reply) error
	ReduceTaskHandler(args *Args, reply *Reply) error
	ReturnIntermediateHandler(args *Args, reply *Reply) error
	ReceiveJobFinishedHandler(args *Args, reply *Reply) error
	ReceiveCheckAliveHandler(args *Args, reply *Reply) error
}

func PRCServer(mode string, address string, class string, instance interface{}) {
	rpc.RegisterName(class, instance) // register 
	if listener, err := net.Listen(mode, address); err != nil { // get the listener
		log.Fatal("ListenTCP error:", err)
	} else {
		go func(){ // start a thread to listen
			for {
				if conn, err := listener.Accept(); err != nil { // block
					log.Fatal("Accept error:", err)
				} else {
					go rpc.ServeConn(conn) // start a thread to deal with the request
				}
			}
		}()
	}
}

func RPCCall(class string, method string, args interface{}, 
	        reply interface{}, address string, is_sync bool) bool {
	client, err := rpc.Dial("tcp", address)
	// defer client.Close()
	if err != nil {
		log.Printf("The device[%s] may be dead. Dial error:%s\n", address, err)
		return false
	}
	rpcname := fmt.Sprintf("%s.%s", class, method)
	if is_sync { 
		err = client.Call(rpcname, args, reply) // sync
		if err != nil {
			log.Printf("The device[%s] may be dead. Call error:%s", address, err)
			return false
		}
	} else {
		client.Go(rpcname, args, reply, nil) // async
	}
	return true
}