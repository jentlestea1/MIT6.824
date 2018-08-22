package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

//		type DoTaskArgs struct {
//			JobName    string
//			File       string   // only for map, the input file
//			Phase      jobPhase // are we in mapPhase or reducePhase?
//			TaskNumber int      // this task's index in the current phase
//
//			// NumOtherPhase is the total number of tasks in other phase; mappers
//			// need this to compute the number of output bins, and reducers needs
//			// this to know how many input files to collect.
//			NumOtherPhase int
//		}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nios = nReduce
	case reducePhase:
		ntasks = nReduce
		nios = len(mapFiles)
	}
	var wg sync.WaitGroup

	fmt.Printf("Schedule: %v %v %v tasks (%d I/Os)\n", ntasks, phase,mapPhase, nios)


	//fmt.Printf("mapFiles:%v\n",mapFiles)
	taskNum := 1
	fmt.Printf("taskNum:%v",&taskNum)
	for i:=0;i<ntasks;i++ {                    
		wg.Add(1)
		go func(task_number int){                   //非闭包结构内部函数不会改变上层函数局部变量的值
			//for {
			rpc_address := <-registerChan
			fmt.Printf("rpc_address: %v\n",rpc_address)
			var ret bool
			//call map work
			//fmt.Printf("task_number:%v\n",task_number)
			args := &DoTaskArgs{jobName,mapFiles[task_number],phase,task_number,nios}
			//fmt.Printf("-----task_number:%v\n",task_number)
			ret = call(rpc_address, "Worker.DoTask",args,nil)
			//fmt.Printf("ret:%v\n",ret)
			//fmt.Printf("task_number:%v",&task_number)
			for {
				if ret==false{
					continue
				}
				wg.Done()
				registerChan <- rpc_address
				break
			}
			//}
		}(i)
	}
	wg.Wait()




//	var wg sync.WaitGroup  //
//	for i := 0; i < ntasks; i++ {
//		wg.Add(1)  // 增加WaitGroup的计数
//		go func(taskNum int, nios int, phase jobPhase) {
//			debug("DEBUG: current taskNum: %v, nios: %v, phase: %v\n", taskNum, nios, phase)
//			for  {
//				worker := <- registerChan  // 获取工作rpc服务器, worker == address
//				debug("DEBUG: current worker port: %v\n", worker)
//	
//				var args DoTaskArgs
//				args.JobName = jobName
//				args.File = mapFiles[taskNum]
//				args.Phase = phase
//				args.TaskNumber = taskNum
//				args.NumOtherPhase = nios
//				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
//				if ok {
//					wg.Done()
//					registerChan <- worker
//					break
//				}  // else 表示失败, 使用新的worker 则会进入下一次for循环重试
//			}
//		}(i, nios, phase)
//	}
//	wg.Wait()  // 等待所有的任务完成

	
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
	return
}
