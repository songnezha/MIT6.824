package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// Add your RPC definitions here.
type task struct {
	Action string
	File   string
}

type worker struct {
	UUID        string
	Status      string
	TaskTimeout time.Time
	Task        *task
}

type Args struct {
	Worker *worker
}

type Reply struct {
	NReduce       int
	IsMapFinished bool
	IsAllFinished bool
	NextWorker    *worker
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
