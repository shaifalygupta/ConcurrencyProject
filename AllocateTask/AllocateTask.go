package AllocateTask

import (
	"path/filepath"
	"os"
	"encoding/csv"
	"strconv"
	"time"
	"fmt"
	"io"
)
// create a struct for reading and print information
type  EPGData  struct{
	ID string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
	SubType string `json:"subtype"`
	Duration int `json:"duration"`
	SourceSystem string `json:"sourcesystem"`
	Country string `json:"country"`
	Channel string `json:"channel"`
}

//create channel buffer to store task created
var EPGDataList = make(chan EPGData,5)


type Result struct {
	EPData         EPGData
}

//create channel buffer to store result from task to be printed
var Results = make(chan Result,5)

// This function reads a csv file line by line and create a task to be done by worker
//and then move csv file to other location after creating task
//and sleep till next csv is coming
func Allocate() {
	count:= 0;
	for  {
		filename,_ := filepath.Abs("../ConcurrencyProject/test.csv")

		// Open CSV file
		if Exists(filename) {
			f, err := os.Open(filename)
			if err != nil {
				//panic(err)
				fmt.Println("Error in File Open")
				time.Sleep(5 * time.Second);
			}
			// Read File into a Variable
			lines, err := csv.NewReader(f).ReadAll()
			if err != nil {
				//panic(err)
				fmt.Println("Error in File Reading")
				time.Sleep(5 * time.Second);
			}
			f.Close()

			// Loop through lines & turn into object
			for i, line := range lines {
				if i > 0 {
					n_value, _ := strconv.Atoi(line[4]);
					EPData := EPGData{line[0], line[1], line[2], line[3], n_value, line[5], line[6], line[7],}
					EPGDataList <- EPData
				}
			}

			//close(EPGDataList)
			//"+strconv.Itoa(count)+"
			nPath, _ := filepath.Abs("../ConcurrencyProject/ReadData/test.csv")
			Copy(filename, nPath)

			if (len(lines) == 0) {
				time.Sleep(5 * time.Second);
				fmt.Println("Mesaage waiting")

			}
		}else {
			time.Sleep(5 * time.Second);
			fmt.Println("Mesaage waiting")
		}
		count++

	}


}

func Copy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	//defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	//defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	in.Close()
	err=os.RemoveAll(src)
	if err != nil {
		fmt.Println(err)
	}
	return out.Close()
}

func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
