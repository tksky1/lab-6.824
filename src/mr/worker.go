package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReducing(task ReduceTask, reducef func(string, []string) string, nMaps int) {
	fmt.Println("开始id为", task.Id, "的reduce工作。")
	intermediate := []KeyValue{}
	for i := 0; i < nMaps; i++ {
		b, err := ioutil.ReadFile("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Id) + ".tmp")
		if err == nil {
			var tmp []KeyValue
			json.Unmarshal(b, &tmp)
			intermediate = append(intermediate, tmp...)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(task.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	fmt.Println("worker完成", task.Id, "号reduce任务")
	Ask4FinishReducing(task.Id)
}

func doMapping(task MapTask, mapf func(string, string) []KeyValue, nReduce int) {
	fmt.Println("开始id为", task.Id, "的map工作。")
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate := make(map[int][]KeyValue)
	for _, keyValue := range kva {
		intermediate[ihash(keyValue.Key)%nReduce] = append(intermediate[ihash(keyValue.Key)%nReduce], keyValue)
	}

	for key, value := range intermediate {
		ofile, _ := os.Create("mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(key) + ".tmp")
		b, _ := json.Marshal(value)
		ofile.Write(b)
		ofile.Close()
	}
	fmt.Println("worker完成map任务", task.Id)
	Ask4FinishMapping(task.Id)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := Ask4Job()
		if reply.ReplyStat == 1 {
			task := reply.MapperTask
			fmt.Println("worker接受到任务", task.Id)
			doMapping(task, mapf, reply.ReduceNum)
		}
		if reply.ReplyStat == 2 {
			task := reply.ReducerTask
			fmt.Println("woker接收到reduce任务", task.Id)
			doReducing(task, reducef, reply.MapNum)
		}
		if reply.ReplyStat == 0 {
			fmt.Println("coordinator not found")
			time.Sleep(500)
		}
		if reply.ReplyStat == 4 {
			fmt.Println("收到退出指令，退出")
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Ask4FinishReducing(taskId int) {
	reply := FinishReducingReply{}
	arg := FinishReducingArg{taskId}
	ok := call("Coordinator.Call4FinishReducing", &arg, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}
}

func Ask4FinishMapping(taskId int) {
	reply := FinishMappingReply{}
	arg := FinishMappingArg{taskId}
	ok := call("Coordinator.Call4FinishMapping", &arg, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}
}

func Ask4Job() Ask4JobReply {
	reply := Ask4JobReply{}
	arg := Ask4JobArg{}
	ok := call("Coordinator.Call4Job", &arg, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return Ask4JobReply{}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
