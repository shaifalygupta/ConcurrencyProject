package AllocateTask

import (
	"path/filepath"
	"os"
	"encoding/csv"
	"strconv"
	"time"
	"fmt"
	"io"
	"io/ioutil"
	"github.com/streadway/amqp"
	"log"
	"strings"
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

// This function reads a csv file or MSMQ Data line by line based on condition mentioned in Config.text file
// and create a task to be done by worker
//For CSV we move csv file to other location after creating task
//and sleep till next csv is coming
func Allocate() {
	//Read data from config file which will decide what will be the source of job creation i.e. MSMQ/CSV
	Configfilename,_ := filepath.Abs("../ConcurrencyProject/Config.txt")
	bs,err:=ioutil.ReadFile(Configfilename)
	if err!=nil{
		fmt.Println("Error:",err)
		os.Exit(1)
	}
	//strings.Split(string(bs),",")
	n_Config:=string(bs)


	if n_Config == "MSMQ"{
		AllocateByMSMQ()
	}else if n_Config == "CSV" {
		AllocateByCSV()
	}else {
		fmt.Println("Please mention Source as CSV or MSMQ for creating jobs")
		os.Exit(1)
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


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func AllocateByCSV(){
	count:= 0;
	for {
		filename, _ := filepath.Abs("../ConcurrencyProject/test.csv")

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
		} else {
			time.Sleep(5 * time.Second);
			fmt.Println("Mesaage waiting")
		}

		count++

	}
}

func AllocateByMSMQ(){
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err1 := conn.Channel()
	failOnError(err1, "Failed to open a channel")
	defer ch.Close()

	q, err1 := ch.QueueDeclare(
		"GoQueue", // name
		true,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err1, "Failed to declare a queue")

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	for d := range msgs {
		//log.Printf("Received a message: %s", d.Body)
		//for i, line := range lines {
		//if i > 0 {
		line:=strings.Split(string(d.Body),",")
		n_value, _ := strconv.Atoi(line[4]);
		EPData := EPGData{line[0], line[1], line[2], line[3], n_value, line[5], line[6], line[7],}
		EPGDataList <- EPData
		//}
		//}
	}

}
