package main

import (
	"encoding/csv"
	"os"
	"runtime"
	rmetrics "runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

type metric struct {
	Deliveryid      string
	Eventid         string
	Eventbornat     time.Time
	Endpointid      string
	Endpointurl     string
	Message         string
	Resourceid      string
	Attempttime     time.Time
	Attempterror    error
	Attemptstatus   int
	Attemptduration time.Duration
	Retryat         time.Time
	Lenparked       int
}
type endresponse map[string]string
type logs struct {
	Endpointid  string
	Moment      time.Time
	Qlength     int
	lanelengths []int
	inflight    int64
	servicetime int32
	tokencount  int32
	giventokens int32
}
type routines struct {
	Num      int
	Moment   time.Time
	Stats    *runtime.MemStats
	user     float64
	gc       float64
	total    float64
	idle     float64
	scavenge float64
}
type responselog struct {
	time    time.Time
	eventid string
	url     string
	result  string
}

var metrics = make(chan metric, 10000)
var endpointlogs = make(chan logs, 10000)
var gologs = make(chan routines, 10000)
var responsechan = make(chan responselog, 10000)

func gomanager(gologs chan routines, done chan struct{}, waiter *sync.WaitGroup) {
	waiter.Add(1)

	go func() {
		defer waiter.Done()
		samples := []rmetrics.Sample{
			{Name: "/cpu/classes/user:cpu-seconds"},
			{Name: "/cpu/classes/gc/total:cpu-seconds"},
			{Name: "/cpu/classes/total:cpu-seconds"},
			{Name: "/cpu/classes/idle:cpu-seconds"},
			{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
		}
		for {

			rmetrics.Read(samples)
			user := samples[0].Value.Float64()
			gc := samples[1].Value.Float64()
			total := samples[2].Value.Float64()
			idle := samples[3].Value.Float64()
			scavenge := samples[4].Value.Float64()
			stats := &runtime.MemStats{}
			num := runtime.NumGoroutine()
			moment := time.Now()
			runtime.ReadMemStats(stats)

			log := routines{num, moment, stats, user, gc, total, idle, scavenge}
			gologs <- log
			time.Sleep(1 * time.Second)
			select {
			case <-done:
				return

			default:
				continue
			}
		}
	}()
	now := strconv.Itoa(int(time.Now().UnixMilli()))
	now += "goroutinecount.csv"

	f, err := os.Create(now)

	if err != nil {
		panic(err)

	}
	defer f.Close()
	file := csv.NewWriter(f)
	headers := []string{
		"Num", "Moment", "Sys", "HeapInuse", "StackInuse", "HeapObjects", "NumGC", "PauseTotalNs",
		"GCCPUFraction", "User", "Gc", "Total", "Idle", "Scavenge",
	}

	file.Write(headers)
	tick := time.Now()
	for row := range gologs {

		if err != nil {
			panic(err)
		}

		err = file.Write([]string{
			strconv.Itoa(row.Num),
			row.Moment.Format(time.RFC3339Nano), strconv.FormatUint(row.Stats.Sys, 10),
			strconv.FormatUint(row.Stats.HeapInuse, 10),
			strconv.FormatUint(row.Stats.StackInuse, 10),
			strconv.FormatUint(row.Stats.HeapObjects, 10),
			strconv.FormatUint(uint64(row.Stats.NumGC), 10),
			strconv.FormatUint(row.Stats.PauseTotalNs, 10),
			strconv.FormatFloat(row.Stats.GCCPUFraction, 'f', 6, 64),
			strconv.FormatFloat(row.user, 'f', 6, 64),
			strconv.FormatFloat(row.gc, 'f', 6, 64),
			strconv.FormatFloat(row.total, 'f', 6, 64),
			strconv.FormatFloat(row.idle, 'f', 6, 64),
			strconv.FormatFloat(row.scavenge, 'f', 6, 64),
		})
		if time.Since(tick) >= 1*time.Second {
			file.Flush()
			tick = time.Now()
		}
		if err != nil {
			panic(err)
		}
	}
	file.Flush()

	logsdone <- 1
}

func endpointlogger(endpointlogs chan logs, done chan struct{}, waiter *sync.WaitGroup) {

	for _, v := range endpoints {

		waiter.Add(1)
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			defer waiter.Done()
			for {

				x := []int{}

				for i := 0; i < int(v.lanecount.Load()); i++ {

					count := int(len(v.lanemap[i]))

					x = append(x, count)
				}

				moment := time.Now()
				length := len(v.queue)
				log := logs{v.id, moment, length, x, v.inflightcount.Load(), v.ratelimit.Load(), v.tokencount.Load(), v.giventokens.Load()}
				endpointlogs <- log

				select {
				case <-v.resizewake:
				case <-ticker.C:

				}

				if v.resizecalled.Load() == true {
					donechan := v.resizedone
					v.resizeack <- 1
					<-donechan
				}
				select {
				case <-done:
					return
				default:
					continue
				}
			}

		}()
	}
	now := strconv.Itoa(int(time.Now().UnixMilli()))
	final := now + "endpointsnap.csv"

	f, err := os.Create(final)

	if err != nil {
		panic(err)

	}
	defer f.Close()
	file := csv.NewWriter(f)

	headers := []string{
		"Endpointid",
		"Moment",
		"Qlength", "lanelengths", "servicetime", "tokencount", "giventokens"}
	file.Write([]string(headers))
	tick := time.Now()
	for row := range endpointlogs {
		parts := make([]string, len(row.lanelengths))
		for i, n := range row.lanelengths {
			parts[i] = strconv.Itoa(n)
		}
		s := strings.Join(parts, ";")

		err = file.Write([]string{
			row.Endpointid,
			row.Moment.Format(time.RFC3339Nano),
			strconv.Itoa(row.Qlength), s, strconv.Itoa(int(row.servicetime)),
			strconv.Itoa(int(row.tokencount)),
			strconv.Itoa(int(row.giventokens))})

		if time.Since(tick) >= 1*time.Second {
			file.Flush()
			tick = time.Now()
		}
		if err != nil {
			panic(err)
		}
	}
	file.Flush()

	logsdone <- 1
}

func performancetracker(rows chan metric) {

	now := strconv.Itoa(int(time.Now().UnixMilli()))
	final := now + "performance.csv"

	f, err := os.Create(final)

	if err != nil {
		panic(err)

	}
	defer f.Close()
	file := csv.NewWriter(f)
	headers := []string{"Deliveryid", "Eventid", "Eventbornat", "Endpointid", "Endpointurl", "Message",
		"Resourceid", "Attempttime",
		"Attempterror",
		"Attemptstatus",
		"Attemptduration",
		"Retryat",
		"Lenparked"}
	file.Write([]string(headers))
	tick := time.Now()
	for row := range rows {
		var atterr string
		if row.Attempterror == nil {
			atterr = "0"
		} else {
			atterr = row.Attempterror.Error()
		}

		err = file.Write([]string{row.Deliveryid,
			row.Eventid,
			row.Eventbornat.Format(time.RFC3339Nano),
			row.Endpointid,
			row.Endpointurl,
			row.Message,
			row.Resourceid,
			row.Attempttime.Format(time.RFC3339Nano),
			atterr,
			strconv.Itoa(row.Attemptstatus),
			strconv.Itoa(int(row.Attemptduration.Milliseconds())),
			row.Retryat.Format(time.RFC3339Nano),
			strconv.Itoa(row.Lenparked)})

		if time.Since(tick) >= 1*time.Second {
			file.Flush()
			tick = time.Now()
		}
		if err != nil {
			panic(err)
		}

	}
	file.Flush()

	logsdone <- 1
}
func refusaltracker(rows chan responselog) {

	now := strconv.Itoa(int(time.Now().UnixMilli()))
	final := now + "refusals.csv"

	f, err := os.Create(final)

	if err != nil {
		panic(err)

	}
	defer f.Close()
	file := csv.NewWriter(f)
	headers := []string{"time", "eventid", "url", "result"}
	file.Write([]string(headers))
	tick := time.Now()
	for row := range rows {
		err = file.Write([]string{row.time.Format(time.RFC3339Nano),
			row.eventid, row.url, row.result})

		if time.Since(tick) >= 1*time.Second {
			file.Flush()
			tick = time.Now()
		}
		if err != nil {
			panic(err)
		}

	}
	file.Flush()

	logsdone <- 1
}
