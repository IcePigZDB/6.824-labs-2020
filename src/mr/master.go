package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady   uint = 0 // need init
	TaskStatusQuene   uint = 1 // after init and quening
	TaskStatusRunning uint = 2 // running by worker
	TaskStatusFinish  uint = 3 // finish
	TaskStatusErr     uint = 4 // err
)

const (
	// TODO MaxTaskRunTime set to under 1s,it can not pass map_parallelism test and map parallelism test
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type TaskStatus struct {
	Status    uint
	WorkerID  uint      // worker do it
	StartTime time.Time // start time
}
type Master struct {
	// Your definitions here.
	files     []string
	workerSeq uint // [0,
	nMaps     uint // number of input file
	nReduce   uint // number of Reduce
	taskPhase TaskPhase
	taskCh    chan Task    // task ch max(nMaps,nReduce)
	taskStats []TaskStatus // task status slice
	done      bool
	mu        sync.Mutex
}

func (m *Master) getTask(taskID uint) Task {
	task := Task{
		Filename: "",
		Seq:      taskID,
		Phase:    m.taskPhase,
		NMaps:    m.nMaps,
		NReduce:  m.nReduce,
		Alive:    true}
	DPrintf("in getTask: askCh长度:%d。当前TaskID:%d。\n当前taskStatus状态%+v。", len(m.taskCh), taskID, m.taskStats)
	// MapPhase task is split one file into NRduce file
	if m.taskPhase == MapPhase {
		task.Filename = m.files[taskID]
	}

	return task
}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	// default 0 = TaskStatusReady
	m.taskStats = make([]TaskStatus, m.nMaps)
}
func (m *Master) initReduceTask() {
	m.taskPhase = ReducePhase
	// default 0 = TaskStatusReady
	m.taskStats = make([]TaskStatus, m.nReduce)
}

func (m *Master) reQueneTask(taskID uint) {
	task := m.getTask(taskID)
	m.taskCh <- task
	DPrintf("in reQueneTask: put task:%d to channel len(ch):%d", task.Seq, len(m.taskCh))
	m.taskStats[taskID].Status = TaskStatusQuene
}
func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return
	}
	allFinish := true
	for index, stat := range m.taskStats {
		DPrintf("%d,len(ch):%d", index, len(m.taskCh))
		switch stat.Status {
		case TaskStatusReady:
			allFinish = false
			m.reQueneTask(uint(index))

		case TaskStatusQuene:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			// running timeout
			if time.Now().Sub(stat.StartTime) > MaxTaskRunTime {
				fmt.Println("running timeout")
				m.reQueneTask(uint(index))
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			m.reQueneTask(uint(index))
		}
	}
	// phase finish
	if allFinish {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) tickSchedule() {
	for {
		if !m.Done() {
			go m.schedule()
			time.Sleep(ScheduleInterval)
		}
	}
}
func (m *Master) regTask(args *ReqTaskArgs, task Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// it will happen if MaxTaskRunTume small than 2
	if task.Phase != m.taskPhase {
		panic("report task error,report task's phase not equal with master phase.")
	}
	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerID = args.WorkerID
	m.taskStats[task.Seq].StartTime = time.Now()
}
func (m *Master) fetchTask(args *ReqTaskArgs) Task {
	for {
		task := <-m.taskCh
		DPrintf("in fetchTask: fetch task %d from taskCh for workerID:%d,task phase:%s,done%s", task.Seq, args.WorkerID, task.Phase, m.done)
		if m.taskStats[task.Seq].Status != TaskStatusFinish {
			return task
		}
		DPrintf("in fetchTask: task ch clear taskID:%d,after len(ch):%d", task.Seq, len(m.taskCh))
	}
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

func (m *Master) ReqOneTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	task := m.fetchTask(args)
	reply.Task = &task

	if task.Alive {
		m.regTask(args, task)
	}
	DPrintf("in ReqOneTask: %d fetch taskID%d in pahse%s,left len(ch):%d", args.WorkerID, task.Seq, task.Phase, len(m.taskCh))
	// DPrintf("in ReqOneTask,args:%+v,reply:%+v", args, reply)
	return nil
}

func (m *Master) RegWorker(args *RegWorkerArgs, reply *RegWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.WorkerID = m.workerSeq
	m.workerSeq++
	return nil
}
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// DPrintf("m phase:%v", m.taskPhase)
	// ignore outtime task report
	// TODO will a worker stop and overwrite other's work ? yes
	// and we do not consider a part or fail overwrite.
	// if a task has ben done by others
	DPrintf("in ReportTask: worker%d report taskID%d in phase%s status=%s", args.WorkerID, args.Seq, args.Phase, args.Done)
	if args.Phase != m.taskPhase || args.WorkerID != m.taskStats[args.Seq].WorkerID || m.taskStats[args.Seq].Status == TaskStatusFinish {
		return nil
	}
	// change taskStatus
	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}
	// check phase done or requene task imediately
	// it can be remove for tickSchedule
	go m.schedule()
	return nil
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
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		files:   files,
		nMaps:   uint(len(files)),
		nReduce: uint(nReduce),
		done:    false,
		mu:      sync.Mutex{},
	}
	// make a big chan
	if m.nMaps > m.nReduce {
		m.taskCh = make(chan Task, m.nMaps)
	} else {
		m.taskCh = make(chan Task, nReduce)
	}
	// DPrintf("nMaps:%d,nReduce:%d,filename:files%v", m.nMaps, m.nReduce, m.files)
	m.initMapTask()
	go m.tickSchedule()
	m.server()
	return &m
}
