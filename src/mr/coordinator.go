package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	Id        int
	FileName  string
	Stat      int       // 0: not started 1: working 2: done
	StartTime time.Time // for stat 1 only
}

type ReduceTask struct {
	Id        int
	Stat      int       // 0: not started 1: working 2: done
	StartTime time.Time // for stat 1 only
}

type Coordinator struct {
	// Your definitions here.
	taskNum        int //文件数
	nReduce        int //nReduce
	mapTasks       []MapTask
	reduceTasks    []ReduceTask
	mappingDone    bool
	reduceDone     bool
	reduceInitDone bool
	mux            sync.Mutex
}

// 即时检查reduce是否完成
func (c *Coordinator) checkReducingDone() bool {
	for _, task := range c.reduceTasks {
		if task.Stat != 2 {
			return false
		}
	}
	return true
}

// 单独开goRoutine，循环检测任务是否都已经完成，并处理超时任务
func refreshReducing(c *Coordinator) {
	for {
		done := true
		c.mux.Lock()
		for i, task := range c.reduceTasks {
			if task.Stat == 0 {
				done = false
			}
			if task.Stat == 1 {
				done = false
				if time.Since(task.StartTime)/time.Second > 10 {
					c.reduceTasks[i].Stat = 0
					fmt.Println("Reduce任务", task.Id, "超时，正在重新配置")
				}
			}
		}
		if done {
			c.reduceDone = true
			fmt.Println("所有reducing任务已完成，正在发布关闭指令..")
			time.Sleep(500)
			c.mux.Unlock()
			break
		}
		c.mux.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// 准备好reduce任务
func (c *Coordinator) setupReducing() {
	tasks := make([]ReduceTask, 0)
	for i := 0; i < c.nReduce; i++ {
		tasks = append(tasks, ReduceTask{i, 0, time.Now()})
	}
	c.reduceTasks = tasks
	go refreshReducing(c)
	fmt.Println("reduce初始化完成")
	c.reduceInitDone = true
}

// 即时检查map是否完成
func (c *Coordinator) checkMappingDone() bool {
	for _, task := range c.mapTasks {
		if task.Stat != 2 {
			return false
		}
	}
	return true
}

// 单独开goRoutine，循环检测任务是否都已经完成，并处理超时任务
func refreshMapping(c *Coordinator) {
	for {
		done := true
		c.mux.Lock()
		for i, task := range c.mapTasks {
			if task.Stat == 0 {
				done = false
			}
			if task.Stat == 1 {
				done = false
				if time.Since(task.StartTime)/time.Second > 10 {
					c.mapTasks[i].Stat = 0
					fmt.Println("Map任务", task.Id, "超时，正在重新配置")
				}
			}
		}
		if done {
			c.mappingDone = true
			fmt.Println("所有mapping任务已完成")
			c.setupReducing()
			c.mux.Unlock()
			break
		}
		c.mux.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// 准备好初始化，准备好MapTask
func (c *Coordinator) init(files []string, nReduce int) {
	c.taskNum = len(files)
	c.nReduce = nReduce
	tasks := make([]MapTask, 0)
	for i, v := range files {
		tasks = append(tasks, MapTask{i, v, 0, time.Now()})
	}
	c.mapTasks = tasks
	go refreshMapping(c)
	fmt.Println("初始化完毕，接收到", c.taskNum, "个任务")
}

// Call4FinishReducing worker通知已经完成reduce任务
func (c *Coordinator) Call4FinishReducing(arg *FinishReducingArg, reply *FinishReducingReply) error {
	c.mux.Lock()
	c.reduceTasks[arg.Id].Stat = 2
	c.mux.Unlock()
	fmt.Println("收到：", arg.Id, "号reduce已完成")
	return nil
}

// Call4FinishMapping worker通知已经完成mapping任务
func (c *Coordinator) Call4FinishMapping(arg *FinishMappingArg, reply *FinishMappingReply) error {
	c.mux.Lock()
	c.mapTasks[arg.Id].Stat = 2
	c.mux.Unlock()
	fmt.Println("收到：", arg.Id, "号mapping已完成")
	return nil
}

// Call4Job 处理worker的job请求
func (c *Coordinator) Call4Job(arg *Ask4JobArg, reply *Ask4JobReply) error {
	reply.ReplyStat = 3
	c.mux.Lock()
	if !c.mappingDone {
		if !c.checkMappingDone() {
			for i, task := range c.mapTasks {
				if task.Stat == 0 {
					c.mapTasks[i].Stat = 1
					c.mapTasks[i].StartTime = time.Now()
					reply.ReplyStat = 1
					reply.MapperTask = task
					reply.ReduceNum = c.nReduce
					fmt.Println("分配任务", task.Id, "给worker。")
					break
				}
			}
		}

	} else {
		if c.reduceInitDone && !c.reduceDone && !c.checkReducingDone() {
			for i, task := range c.reduceTasks {
				if task.Stat == 0 {
					c.reduceTasks[i].Stat = 1
					c.reduceTasks[i].StartTime = time.Now()
					reply.ReplyStat = 2
					reply.ReducerTask = task
					reply.ReduceNum = c.nReduce
					reply.MapNum = c.taskNum
					fmt.Println("分配reduce任务", task.Id, "给worker。")
					break
				}
			}
		}
	}
	if c.reduceDone {
		reply.ReplyStat = 4
	}
	c.mux.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mux.Lock()
	if c.reduceDone {
		ret = true
	}
	c.mux.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.init(files, nReduce)

	c.server()
	return &c
}
