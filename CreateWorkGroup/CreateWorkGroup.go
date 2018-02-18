package CreateWorkGroup

import (
	"sync"
	"fmt"
Con	"ConcurrencyProject/AllocateTask"
)

//these 3 workers run concurrently and pick the task from EPGDataList Channel buffer and put it in Results channel buffer for printing
func worker(wg *sync.WaitGroup) {
	for job := range Con.EPGDataList {
		output := Con.Result{job}
		Con.Results <- output

	}
	fmt.Println("Job Done")
	wg.Done()
}

//created 3 workers for reading csv file/MSMQ data and do the task
func CreateWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg)
	}
	wg.Wait()
	//close(results)
}