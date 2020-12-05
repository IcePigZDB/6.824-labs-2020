package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
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

type worker struct {
	workerID uint
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

// register to master to get workerID
func (w *worker) register() {
	args := &RegWorkerArgs{}
	reply := &RegWorkerReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("in register worker:" + fmt.Sprint(w.workerID) + "call rpc RegWorker fail.")
	}
	w.workerID = reply.WorkerID
}

// run till server close
func (w *worker) run() {
	// if reqTask conn fail,worker exit
	for {
		DPrintf("workerID:%d begin req for task", w.workerID)
		t := w.reqTask()
		if !t.Alive {
			DPrintf("worker get task not alive,exit")
			return
		}
		w.doTask(t)
	}
}

// req a task
func (w *worker) reqTask() Task {
	args := ReqTaskArgs{WorkerID: w.workerID}
	reply := ReqTaskReply{}
	if ok := call("Master.ReqOneTask", &args, &reply); !ok {
		log.Fatal("in ReqTask worker:" + fmt.Sprint(w.workerID) + " call rpc ReqTask fail.Don't worry it is normally caused by master finish all task and close conn.")
	}
	DPrintf("in reqTask: worker %d fetch s% task %d.", w.workerID, reply.Task.Phase, reply.Task.Seq)
	return *reply.Task
}

// report task status done or err
func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{
		Done:     done,
		Seq:      t.Seq,
		Phase:    t.Phase,
		WorkerID: w.workerID}
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail:%+v", args)
	}
}

// do task{map,reudece}
func (w *worker) doTask(t Task) {
	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		log.Fatal("task phase err")
	}
}

// doMapTask
// read task.Filename hash it into nReduce buffer and write to nReduce file
func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		panic("in doMapTask open file error:" + err.Error())
	}
	// mapf make split contents into kvs
	kvs := w.mapf(t.Filename, string(contents))
	// ihash to corresponding reduceBuffers
	reduceBuffers := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		reduceNum := uint(ihash(kv.Key)) % t.NReduce
		reduceBuffers[reduceNum] = append(reduceBuffers[reduceNum], kv)
	}
	for idx, reduce := range reduceBuffers {
		// open file
		filename := reduceName(t.Seq, uint(idx))
		f, err := os.Create(filename)
		if err != nil {
			panic("in doMapTask,create reduce file fail")
		}
		enc := json.NewEncoder(f)
		for _, kv := range reduce {
			// write to file
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		// close file
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	// normal report
	w.reportTask(t, true, nil)
	DPrintf("in doMapTask: workerID:%d,taskID:%d,map filename%s: done", w.workerID, t.Seq, t.Filename)
}

// read nRedece file and count total number with map[string][]string
// the key is that map[kv.Key] = kv.Value (it required by map and reduce func)
func (w *worker) doReduceTask(t Task) {
	DPrintf("in doRedeuceTask: workerID%d begin to do reduce taskID:%d in pahse:%s", w.workerID, t.Seq, t.Phase)
	// reduce merge nMpas file
	maps := make(map[string][]string)
	for i := 0; i < int(t.NMaps); i++ {
		filename := reduceName(uint(i), t.Seq)
		DPrintf("in doReduceTask: reduce filename:%v", filename)
		file, err := os.Open(filename)
		if err != nil {
			w.reportTask(t, false, err)
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// if not exist init []
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			// append
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	DPrintf("in doReduceTask: reduce point2 begin to reducef")
	// eg: wdb 100 \n word 200 \n count 10
	res := make([]string, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	DPrintf("in doReduceTak: reduce point3 write")
	// write to final file
	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}
	w.reportTask(t, true, nil)
	DPrintf("in doReduceTask: workerID:%d,reduce taskID %d: done", w.workerID, t.Seq)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
}

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
