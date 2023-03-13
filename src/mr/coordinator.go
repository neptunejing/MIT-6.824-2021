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

type Status int

const (
	Mapping Status = iota
	Waiting
	Reducing
	Done
)

type Coordinator struct {
	Status      Status // 状态
	files       []string
	mutex       sync.Mutex // 获取任务的锁
	mapTasks    chan *Task // 待分配 map 任务
	reduceTasks chan *Task // 待分配 reduce 任务
	nReduce     int

	taskMutex      sync.Mutex   // 修改 xxTaskDone 时的锁
	mapTaskDone    map[int]bool // map 任务是否完成
	reduceTaskDone map[int]bool // reduce 任务是否完成
}

func (c *Coordinator) HandleRequest(req *Req, res *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch c.Status {
	case Mapping:
		if len(c.mapTasks) > 0 {
			nextTask := <-c.mapTasks
			*res = *nextTask

			// 分配后任务后放入 TaskDone 哈希表中，协程中判断是否完成
			c.taskMutex.Lock()
			c.mapTaskDone[nextTask.ID] = false
			c.taskMutex.Unlock()

			// 创建协程进行计时，允许 res 先返回
			go func() {
				timer := time.NewTimer(10 * time.Second)
				select {
				case <-timer.C:
					c.taskMutex.Lock()
					if isDone, ok := c.mapTaskDone[nextTask.ID]; ok && !isDone {
						c.mapTasks <- nextTask // 超时未完成则将任务重新放入队列
						fmt.Printf("Map task %v failed and will be reassigned\n", nextTask.ID)
					}
					c.taskMutex.Unlock()
				}
			}()
		} else if len(c.mapTaskDone) == 0 {
			log.Println("Switch to waiting")
			c.Status = Waiting
		}
	case Waiting:
		c.initReduceTasks()
		c.Status = Reducing
	case Reducing:
		if len(c.reduceTasks) > 0 {
			nextTask := <-c.reduceTasks
			*res = *nextTask

			c.taskMutex.Lock()
			c.reduceTaskDone[nextTask.ID] = false
			c.taskMutex.Unlock()

			go func() {
				timer := time.NewTimer(10 * time.Second)
				select {
				case <-timer.C:
					c.taskMutex.Lock()
					if isDone, ok := c.reduceTaskDone[nextTask.ID]; !isDone && ok {
						c.reduceTasks <- nextTask
						fmt.Printf("Reduce task %v failed and will be reassigned\n", nextTask.ID)
					}
					c.taskMutex.Unlock()
				}
			}()
		} else if len(c.reduceTaskDone) == 0 {
			// 当所有文件输出时，coordinator 和 workers 才可以退出
			*res = Task{Type: "Done"}
			time.Sleep(time.Second * 5)
			c.Status = Done
		}
	default:
		return nil
	}
	return nil
}

func (c *Coordinator) HandleCheck(check *Check, res *Task) error {
	c.taskMutex.Lock()
	switch check.Type {
	case "Map":
		delete(c.mapTaskDone, check.TaskID)
		fmt.Printf("Map task %v has been done by worker %v\n", check.TaskID, check.WorkerID)
	case "Reduce":
		delete(c.reduceTaskDone, check.TaskID)
		fmt.Printf("Reduce task %v has been done by worker %v\n", check.TaskID, check.WorkerID)
	}
	c.taskMutex.Unlock()
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
	if c.Status == Done {
		log.Println("Coordinator exits")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Status:         Mapping,
		files:          files,
		nReduce:        nReduce,
		mapTasks:       make(chan *Task, len(files)),
		reduceTasks:    make(chan *Task, nReduce),
		mapTaskDone:    make(map[int]bool),
		reduceTaskDone: make(map[int]bool),
	}

	c.initMapTasks()

	c.server()
	return &c
}

// 添加 map 任务到队列
func (c *Coordinator) initMapTasks() {
	for idx, file := range c.files {
		task := Task{
			Type:     "Map",
			FileName: file,
			ID:       idx,
			NReduce:  c.nReduce,
		}
		c.mapTasks <- &task
	}
}

// 添加 reduce 任务到队列
func (c *Coordinator) initReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			Type:     "Reduce",
			ID:       i,
			FileName: "",
			NReduce:  c.nReduce,
		}
		c.reduceTasks <- &task
	}
}
