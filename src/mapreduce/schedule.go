package mapreduce

import (
	"fmt"
	"sync"
)

type WorkerCount struct {
	lock sync.Mutex
	workers map[string]int
}

func (wc *WorkerCount) GetWorkerFailure (worker string) int{
	wc.lock.Lock()
	defer wc.lock.Unlock()

	return wc.workers[worker]
}

func (wc *WorkerCount) WorkerFailuerInc (worker string) {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	wc.workers[worker] ++
}

const MAXWORKERFAILUER = 5

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup
	retryChan := make(chan *DoTaskArgs, ntasks)
	tasksList := make([] *DoTaskArgs, 0)
	usefulWorkChan := make(chan string, ntasks)
	wc := WorkerCount{workers: make(map[string]int)}

	for i := 0; i < ntasks; i++ {
		taskArgs := &DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		tasksList = append(tasksList, taskArgs)
	}

	for len (tasksList) > 0 {
		select {

		case taskArgs := <- retryChan:
			tasksList = append(tasksList, taskArgs)

		case worker := <- registerChan:
			usefulWorkChan <- worker

		case worker := <- usefulWorkChan:
			if wc.GetWorkerFailure(worker) < MAXWORKERFAILUER {
				wg.Add(1)
				index := len(tasksList) - 1
				args := tasksList[index]
				go func (taskArgs *DoTaskArgs, worker string) {
					fmt.Printf("Schedule: %v task #%d to %s worker", phase, taskArgs.TaskNumber, worker)
					defer wg.Done()
					m := call(worker, "Worker.DoTask", taskArgs, nil)
					usefulWorkChan <- worker
					if ! m {
						fmt.Printf("Schedule: %v task #%d failed to execute by %s\n", phase, args.TaskNumber, worker)
						wc.WorkerFailuerInc(worker)
						retryChan <- taskArgs
					}
				}(args, worker)
				tasksList = tasksList[:index]
			}else {
				fmt.Printf("Schedule: %v, Worker %s failed %d times and will no longer be used\n", phase, worker, MAXWORKERFAILUER)
			}
		}
	}


	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
