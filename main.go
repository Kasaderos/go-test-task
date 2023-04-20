package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Data struct {
	A int64 `json:"a"`
	B int64 `json:"b"`
}

type Job struct {
	Data  []Data
	Start int
	End   int
}

func sumBlock(data []Data, start, end int) int64 {
	sum := int64(0)
	for i := start; i < end; i++ {
		sum += data[i].A + data[i].B
	}
	return sum
}

func worker(ctx context.Context, jobC <-chan Job, resultC chan<- int64, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobC:
			if !ok {
				return
			}
			sum := sumBlock(job.Data, job.Start, job.End)
			resultC <- sum
		}
	}
}

func main() {
	var numWorkers int
	var jobQueueSize int
	var fileName string
	flag.IntVar(&numWorkers, "n", 4, "number of workers")
	flag.IntVar(&jobQueueSize, "q", 4, "job queue size")
	flag.StringVar(&fileName, "f", "data.json", "number of workers")
	flag.Parse()

	ctx, cancelCtx := context.WithCancel(context.Background())
	go initGracefulShutDown(cancelCtx)

	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
	}

	var data []Data
	if err := json.Unmarshal(bytes, &data); err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	resultC := make(chan int64)
	jobC := make(chan Job, jobQueueSize)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, jobC, resultC, &wg)
	}

	// send blocks
	go func() {
		blockSize := 100
	LP:
		for i := 0; i < len(data); i += blockSize {
			start := i
			end := i + blockSize
			if end > len(data) {
				end = len(data)
			}
			job := Job{
				Data:  data,
				Start: start,
				End:   end,
			}
			select {
			case <-ctx.Done():
				break LP
			case jobC <- job:
			}
		}
		close(jobC)

		wg.Wait()
		close(resultC)
	}()

	sum := int64(0)
	for s := range resultC {
		sum += s
	}
	fmt.Println("Sum:", sum, "Time:", time.Since(t))
}

func initGracefulShutDown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	log.Println("graceful shutdown", <-sigChan)
	cancel()
}
