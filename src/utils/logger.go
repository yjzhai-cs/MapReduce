package utils
// reference https://blog.josejg.com/debugging-pretty/

import "os"
import "time"
import "log"
import "fmt"
import "strconv"
import "sync"
import "6.5840/configs"

type logTopic string
const (
	Client  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

var topics []logTopic = []logTopic{"CLNT", "CMIT", "DROP", "ERRO", "INFO", "LEAD", 
					"LOG1", "LOG2", "PERS", "SNAP", "TERM", "TEST", "TIMR", "VOTE", "TRCE", "WARN"}

var debugStart time.Time
var debugVerbosity int
var mutex sync.Mutex

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func InitLogger() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debugger(topic logTopic, format string, a ...interface{}) {
	if !configs.KVRAFT_DEBUG && !configs.RAFT_DEBUG { return } // close debug
	
	// if debug >= 1 {
		mutex.Lock()
		time := time.Since(debugStart).Microseconds()
		mutex.Unlock()
		time /= 100
		
		if topic == DPersist { return }
		if topic == DDrop { return }

		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	// }
}