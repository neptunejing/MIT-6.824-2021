package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// worker 向 coordinator 请求任务
type Req struct {
	WorkerID int // 发出请求的 worker ID
}

// coordinator 向 workers 分发任务
type Task struct {
	Type     string // 任务类型
	FileName string // 文件名
	ID       int    // 任务 ID
	NReduce  int    // Reducer 数量
}

// worker 向 coordiantor 确认执行完毕
type Check struct {
	TaskID   int    // 任务 ID
	Type     string // 任务类型
	WorkerID int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
