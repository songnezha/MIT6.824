package mr

import (
	"errors"
	"strconv"
	"sync"
	"time"
)
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	Initialized		bool
	activeWorkers	map[string]*worker
	todoTasks		[]task
	mapWorkers		int
	mapTasks		int
	reduceWorkers	int
	reduceTasks		int
	nReduce			int
	mu 				sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Init(files []string, nReduce int) {
	m.Initialized = true
	m.activeWorkers = map[string]*worker{}
	m.todoTasks = []task{}
	m.nReduce = nReduce
	m.InitStatus()
	m.CreateLocalFiles(files, nReduce)
}

func (m *Master) InitStatus() {
	m.mu.Lock()
	m.mapTasks = 0
	m.mapWorkers = 0
	m.reduceTasks = 0
	m.reduceWorkers = 0
	m.mu.Unlock()
}

func (m *Master) CreateLocalFiles(files []string, nReduce int) {
	var tmpMaps []string
	for i:=0; i!=nReduce; i++ {
		output := "mr-out-" + strconv.Itoa(i+1)
		tmpMap := "mr-tmp-map-" + strconv.Itoa(i+1)
		tmpMaps = append(tmpMaps, tmpMap)

		if _,e:=os.Create(output); e!=nil {
			log.Printf("[Master] FILE %s Create Succeed", output)
		}
		if _,e:=os.Create(tmpMap); e!=nil {
			log.Printf("[Master] FILE %s Create Succeed", tmpMap)
		}
	}
	m.AddTask("map", files...)
	m.AddTask("reduce", tmpMaps...)
}

func (m *Master) AddTask(action string, files ...string) {
	var tasks []task
	for _,file:=range files {
		tasks = append(tasks, task{
			Action: action,
			File: file,
		})
	}
	log.Printf("Add Task %s", tasks)

	m.mu.Lock()
	switch action {
	case "map":
		m.todoTasks = append(tasks, m.todoTasks...)
	break
	case "reduce":
		m.todoTasks = append(m.todoTasks, tasks...)
	break
	}
	m.mu.Unlock()
	log.Printf("[Master] TODO Tasks %s", m.todoTasks)

	m.UpdateStatus()
}

func (m *Master) UpdateStatus() {
	go func ()  {
		m.InitStatus()
		m.mu.Lock()
		for _,w := range m.activeWorkers{
			if w.Task != nil{
				switch w.Task.Action {
				case "map":
					m.mapWorkers++
					break;
				case "reduce":
					m.reduceWorkers++
					break
				}
			}
		}
		for _,t := range m.todoTasks{
			switch t.Action {
			case "map":
				m.mapTasks++
				break
			case "reduce":
				m.reduceTasks++
				break
			}
		}
		m.mu.Unlock()
	}()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Auth(worker *worker) error {
	if time.Now().After(worker.TaskTimeout) {
		return errors.New("[Master] Timeout")
	}
	return nil
}

func (m *Master) RemoveTask(task *task) {
	if task==nil { return }
	m.mu.Lock()
	for i,t := range m.todoTasks {
		if t.File == task.File && t.Action == task.Action {
			log.Printf("[Master] Remove Finished Task")
			m.todoTasks = append(m.todoTasks[:i], m.todoTasks[i+1:]...)
		}
	}
	m.mu.Unlock()
	m.UpdateStatus()
}

func (m *Master) RemoveWorker(worker *worker) {
	m.mu.Lock()
	if _,ok := m.activeWorkers[worker.UUID]; ok{
		delete(m.activeWorkers, worker.UUID)
	}
	m.mu.Unlock()
	m.UpdateStatus()
}

func (m *Master) AddWorker(current *worker) (NextWorker *worker) {
	NextWorker = nil
	m.mu.Lock()
	if len(m.todoTasks) > 0 {
		t := m.todoTasks[0]
		m.todoTasks = m.todoTasks[1:]
		w := worker{
			UUID:			current.UUID,
			Status:			"processing",
			Task: 			&t,
			TaskTimeout:	time.Now().Add(time.Second*10),
		}
		m.activeWorkers[current.UUID] = &w
		NextWorker = &w
	}
	m.mu.Unlock()
	m.UpdateStatus()
	return
}

func (m *Master) Sycn(args *Args, reply *Reply) error {
	switch args.Worker.Status {
	case "idle":
		reply.NextWorker = m.AddWorker(args.Worker)
	break
	case "processing":
		if e := m.Auth(args.Worker); e != nil {
			return e
		}
	break
	case "finished":
		m.RemoveTask(args.Worker.Task)
		m.RemoveWorker(args.Worker)
		reply.NextWorker = &worker{
			UUID:			args.Worker.UUID,
			Status:			"idle",
			TaskTimeout:	time.Time{},
			Task:			nil,
		}
	break
	}
	reply.NReduce = m.nReduce
	reply.IsMapFinished = false
	reply.IsAllFinished = false
	m.mu.Lock()
	if m.mapTasks==0 && m.mapWorkers==0 && m.Initialized {
		reply.IsMapFinished = true
	}
		if m.mapTasks==0 && m.mapWorkers==0 && m.reduceTasks==0 && m.reduceWorkers==0 {
		reply.IsAllFinished = true
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) Checker() {
	go func ()  {
		for{
			log.Printf("[Master] [Checker] Map Workers %d, Map Todo Tasks %d, Reduce Workers %d, Reduce Todo Tasks %d",
				m.mapWorkers, m.mapTasks, m.reduceWorkers, m.reduceTasks)

			m.mu.Lock()
			for _,w := range m.activeWorkers {
				if time.Now().After(w.TaskTimeout) {
					log.Printf("[Master] Remove Timeout Worker %v", w)
					go m.RemoveWorker(w)
					go m.AddTask(w.Task.Action, w.Task.File)
				}
			}
			m.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.mapTasks==0 && m.mapWorkers==0 && m.reduceTasks==0 && m.reduceWorkers==0 && m.Initialized {
		return true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Init(files, nReduce)
	m.Checker()

	m.server()
	return &m
}
