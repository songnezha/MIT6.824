package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Bykey []KeyValue

func (a Bykey) Len() int           { return len(a) }
func (a Bykey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Bykey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type IJ struct {
	I int
	J int
}

type service struct {
	current       *worker
	w             chan *worker
	NReduce       int
	IsMapFinished bool
	IsAllFinished bool
	mapf          func(string, string) []KeyValue
	reducef       func(string, []string) string
}

var (
	wg sync.WaitGroup
	s  = &service{}
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (s *service) Reset() {
	s.current.Status = "idle"
	s.current.Task = nil
	s.current.TaskTimeout = time.Time{}
}

func (s *service) Sync() {
	if s.current == nil {
		s.Reset()
		return
	}
	args := Args{Worker: s.current}
	reply := Reply{}
	if !call("Master.Sync", &args, &reply) {
		os.Exit(0)
	}
	if reply.NextWorker == nil {
		log.Printf("[Worker] Recieve a nil Worker")
	} else {
		s.current = reply.NextWorker
		s.w <- reply.NextWorker
	}
	s.NReduce = reply.NReduce
	s.IsAllFinished = reply.IsAllFinished
	s.IsMapFinished = reply.IsMapFinished
	if reply.IsAllFinished {
		wg.Done()
	}
}

func UUIDgen() (uuid string) {
	unix32bits := uint32(time.Now().UTC().Unix())
	buff := make([]byte, 12)
	numRead, err := rand.Read(buff)
	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}

func (s *service) Init(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	s.w = make(chan *worker)
	s.current = &worker{
		UUID:   UUIDgen(),
		Status: "idle",
		Task:   nil,
	}
	s.mapf = mapf
	s.reducef = reducef
}

func (s *service) Map() {
	t := s.current.Task
	file, err := os.Open(t.File)
	if err != nil {
		log.Fatalf("[Worker] Can't Open file %s", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker] Can't Read file %s", file)
	}
	_ = file.Close()

	kva := s.mapf(t.File, string(content))

	for _, kv := range kva {
		s := "mr-tmp-map-" + strconv.Itoa(ihash(kv.Key)%s.NReduce+1)
		outputFile, err := os.OpenFile(s, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			log.Print(err)
		}
		enc := json.NewEncoder(outputFile)
		e := enc.Encode(&kv)
		if e != nil {
			log.Print(e)
		}
		_ = outputFile.Close()
	}
}

func (s *service) Reduce() {
	t := s.current.Task
	var intermediate []KeyValue
	intermediateFile, _ := os.Open(t.File)
	dec := json.NewDecoder(intermediateFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	sort.Sort(Bykey(intermediate))

	localCache := "[inter-tmp]" + t.File
	file, err := os.OpenFile(localCache, os.O_WRONLY|os.O_TRUNC, os.ModeAppend)
	defer func() {
		_ = file.Close()
	}()
	ij := IJ{}
	if err != nil && os.IsNotExist(err) {
		file, _ = os.Create(localCache)
		ij.I = 0
		ij.J = 0
	} else {
		dec := json.NewDecoder(file)
		_ = dec.Decode(&ij)
	}
	for ij.I < len(intermediate) {
		enc := json.NewEncoder(file)
		_ = enc.Encode(&ij)

		ij.J = ij.I + 1
		for ij.J < len(intermediate) && intermediate[ij.J].Key == intermediate[ij.I].Key {
			ij.J++
		}
		var values []string
		for k := ij.I; k < ij.J; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := s.reducef(intermediate[ij.I].Key, values)

		s := "mr-out-" + strconv.Itoa(ihash(intermediate[ij.I].Key)%s.NReduce+1)
		outputFile, err := os.OpenFile(s, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			log.Print(err)
		}
		// this is the correct format for each line of Reduce Output.
		_, e := fmt.Fprintf(outputFile, "%v %v\n", intermediate[ij.I].Key, output)
		if e != nil {
			log.Print(e)
		}
		_ = outputFile.Close()
		ij.I = ij.J
	}
}

func (s *service) Schedule() {
	go func() {
		for {
			s.Sync()
			time.Sleep(time.Second)
		}
	}()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	s.Init(mapf, reducef)
	wg.Add(1)
	s.Schedule()

	go func() {
		for w := range s.w {
			wg.Add(1)
			if t := w.Task; t != nil {
				log.Printf("[Worker] Working with a %s Task using %s File", t.Action, t.File)
				switch s.current.Task.Action {
				case "map":
					s.Map()
					break
				case "reduce":
					for {
						if !s.IsMapFinished {
							log.Printf("[Worker] Wating for Map to Finish")
							time.Sleep(time.Second)
						} else {
							break
						}
					}
					s.Reduce()
					break
				}
				log.Printf("[Worker] Finished %s Task using %s File", t.Action, t.File)
				s.current.Status = "finished"
			}
			wg.Done()
		}
	}()
	wg.Wait()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
