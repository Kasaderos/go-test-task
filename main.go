package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

type Data struct {
	A int64 `json:"a"`
	B int64 `json:"b"`
}

func sumBlock(data []Data, start, end int, resultC chan<- int64) {
	// data read-only
	sum := int64(0)
	for i := start; i < end; i++ {
		sum += data[i].A + data[i].B
	}
	resultC <- sum
}

func main() {
	var numWorkers int
	flag.IntVar(&numWorkers, "n", 4, "number of workers")
	flag.Parse()

	bytes, err := ioutil.ReadFile("data.json")
	if err != nil {
		log.Fatal(err)
	}

	var data []Data
	if err := json.Unmarshal(bytes, &data); err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	ch := make(chan int64)

	blockSize := 100
	var wg sync.WaitGroup
	for i := 0; i < len(data); i += blockSize {
		start := i
		end := i + blockSize
		if end > len(data) {
			end = len(data)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sumBlock(data, start, end, ch)
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	sum := int64(0)
	for s := range ch {
		sum += s
	}

	fmt.Println("Sum:", sum, "Time:", time.Since(t))
}
