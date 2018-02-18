package main

import (
	"fmt"
	Con "ConcurrencyProject/AllocateTask"
	Crw "ConcurrencyProject/CreateWorkGroup"
)
/**
This Go  file is used to allocate the jobs and also calls the function
to create Workers pool to process jobs .
 */
func main() {

	//forever := make(chan bool)

	go Con.Allocate ()

	// This GO routine reads the results channel to print the result.
	// The result channel is filled by workers with result of the jobs.

	go func() {
		for j := range Con.Results {
			fmt.Println("Mesaage Received  ",j.EPData)
		}

	}()
	noOfWorkers := 3
	Crw.CreateWorkerPool(noOfWorkers)

	//<- forever
}

