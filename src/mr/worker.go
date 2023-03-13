package mr

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rand.Seed(time.Now().UnixNano())
	workerID := rand.Intn(1000)
	log.Printf("Worker %v starts", workerID)

	for {
		task := CallPullTask(workerID)
		switch task.Type {
		case "Map":
			handleMap(mapf, workerID, task)
		case "Reduce":
			handleReduce(reducef, workerID, task)
		case "Done":
			return
		}
		time.Sleep(time.Second)
	}
}

func handleMap(mapf func(string, string) []KeyValue, workerID int, task *Task) {
	filename := task.FileName
	nReduce := task.NReduce

	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	hashKva := make([][]KeyValue, nReduce)
	// 根据 key 把所有 entries 哈希成 nReduce 份
	for _, entry := range kva {
		hashKva[ihash(entry.Key)%nReduce] = append(hashKva[ihash(entry.Key)%nReduce], entry)
	}
	// 生成 nReduce 个临时文件
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp("", "temp")
		if err != nil {
			log.Fatalf("cannot create temp files")
		}
		for _, entry := range hashKva[i] {
			fmt.Fprintf(tempFile, "%v %v\n", entry.Key, entry.Value)
		}
		err = os.Rename(tempFile.Name(), "mr-"+strconv.Itoa(task.ID)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("rename file error")
		}
		tempFile.Close()
	}
	CallCheck(task.ID, "Map", workerID)
}

func handleReduce(reducef func(string, []string) string, workerID int, task *Task) {
	taskID := strconv.Itoa(task.ID)
	files, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("Read intermediate files error")
	}
	kv := make([]KeyValue, 0)
	values := make(map[string][]string)
	for _, fileInfo := range files {
		// 只打开名为 mr-x-taskID 中间文件
		if !strings.HasSuffix(fileInfo.Name(), taskID) {
			continue
		}
		file, err := os.Open(fileInfo.Name())
		if err != nil {
			log.Fatalf("cannot open %v", file.Name())
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", file.Name())
		}
		strContent := string(content)
		strSlice := strings.Split(strContent, "\n")
		// 整理成 <"xxx", ["1", "1", ..., "1"]> 形式的记录
		for _, row := range strSlice {
			kvSlice := strings.Split(row, " ")
			// kvSlice 可能为 [""]
			if len(kvSlice) == 2 {
				values[kvSlice[0]] = append(values[kvSlice[0]], kvSlice[1])
			}
		}
	}
	for k, v := range values {
		kv = append(kv, KeyValue{Key: k, Value: reducef(k, v)})
	}
	outputFile, err := os.CreateTemp("", "temp"+strconv.Itoa(rand.Int()))
	if err != nil {
		log.Fatalf("cannot create temp files")
	}
	sort.Sort(ByKey(kv))
	for _, entry := range kv {
		fmt.Fprintf(outputFile, "%v %v\n", entry.Key, entry.Value)
	}
	err = os.Rename(outputFile.Name(), "mr-out-"+strconv.Itoa(task.ID))
	if err != nil {
		log.Fatalf("rename file error")
	}
	outputFile.Close()
	CallCheck(task.ID, "Reduce", workerID)
}

func CallPullTask(workerID int) *Task {
	req := Req{WorkerID: workerID}
	res := Task{}
	call("Coordinator.HandleRequest", &req, &res)
	return &res
}

func CallCheck(taskID int, taskType string, workerID int) {
	check := Check{
		TaskID:   taskID,
		Type:     taskType,
		WorkerID: workerID,
	}
	res := Task{}
	call("Coordinator.HandleCheck", &check, &res)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
