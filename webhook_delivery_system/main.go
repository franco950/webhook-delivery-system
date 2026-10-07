package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func register(url string) *endpoint {

	id := uuid.New()
	newid := id.String()
	lanemap := make(map[int]chan *delivery)
	laneresources := make(map[int]chan *delivery)
	for y := range configs.lanecount {
		lanemap[int(y)] = make(chan *delivery, configs.endpointcap)
		laneresources[int(y)] = make(chan *delivery, configs.retrychansize)

	}

	x := &endpoint{
		id:             newid,
		lanemap:        lanemap,
		queue:          make(chan *delivery, configs.endpointcap),
		url:            url,
		parked:         []*delivery{},
		lanecount:      atomic.Int32{},
		consecfailures: atomic.Int32{},
		ratelimit:      atomic.Int32{},
		tokenchan:      make(chan int32, configs.tokencap),
		tokencount:     atomic.Int32{},
		giventokens:    atomic.Int32{},
		laneresources:  laneresources,
		resizecalled:   atomic.Bool{},
	}
	x.ratelimit.Store(int32(configs.initratelimit))
	x.resourcemap = make(map[string][]*delivery)
	x.lanecount.Store(configs.lanecount)

	endpointmap[newid] = x

	for range configs.tokencount {
		x.tokenchan <- 1
	}

	x.resizewake = make(chan int)

	x.resizeack = make(chan int)
	x.tokencount.Store(int32(configs.tokencount))
	x.giventokens.Store(int32(configs.tokencount))

	return x

}

func drainhold() {
	for _, v := range endpoints {

		for len(v.queue) > 0 {
			time.Sleep(5 * time.Second)

		}

		lane := v.lanemap
		for _, v := range lane {
			if len(v) > 0 {
				time.Sleep(5 * time.Second)
				drainhold()
			}

		}

		v.mu.Lock()
		maplen := len(v.resourcemap)
		v.mu.Unlock()
		for maplen > 0 {

			time.Sleep(5 * time.Second)
			v.mu.Lock()
			maplen = len(v.resourcemap)
			v.mu.Unlock()

		}

		resources := v.laneresources

		for _, v := range resources {
			if len(v) > 0 {
				time.Sleep(5 * time.Second)
				drainhold()
			}

		}
		for v.holding.Load() > 0 {
			time.Sleep(5 * time.Second)
		}

	}

}
func logdrain() {

	for range 4 {
		<-logsdone

	}

}

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	custom.MaxIdleConnsPerHost = 1000
	custom.MaxIdleConns = 1000
	custom.IdleConnTimeout = 10 * time.Second

	endpointcount := 0

	for range Numendpoints {
		url := strconv.Itoa(endpointcount)
		var endpoint1 = register("http://127.0.0.1:8080/" + url)
		endpoints = append(endpoints, endpoint1)
		endpointcount += 1
	}

	done := make(chan struct{})

	waiter := &sync.WaitGroup{}
	go Generate()
	go performancetracker(metrics)
	go gomanager(gologs, done, waiter)
	go endpointlogger(endpointlogs, done, waiter)
	go ReceiveEvents(EventChannel, done)
	go refusaltracker(responsechan)
	for _, endpoint := range endpoints {

		go endpoint.lanework()
		go endpoint.retrywork()

	}

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("quit signal received, waiting for queues to drain")
	close(draincalled)
	drainhold()
	close(done)
	waiter.Wait()
	close(responsechan)
	close(metrics)
	close(gologs)
	close(endpointlogs)
	logdrain()

}
