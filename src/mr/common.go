package mr

import "fmt"

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const Debug = false

// const Debug = true

func DPrintf(formate string, v ...interface{}) {
	if Debug {
		fmt.Printf(formate+"\n", v...)
	}
}

// Task represents a map/recude task
type Task struct {
	Seq      uint //task id
	Phase    TaskPhase
	Filename string // map use to transfer file need to  map
	NReduce  uint
	NMaps    uint
	Alive    bool // TODO a feature api,useless now
}

func reduceName(mapIdx uint, reduceIdx uint) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx uint) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
