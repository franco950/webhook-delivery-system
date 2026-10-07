package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type sender struct {
	at    int
	event event
}

var holdingchannel = make(chan sender, 300001)
var EventChannel = make(chan event)

// //Merchant group	Share of events
// Backend merchants 	70%
// ERP merchants 	5%
// Shared-hosting merchants 	10%
// Merchants with no endpoint 	15%

// mapping event id to endpoint ids
var Subscriptions = map[string][]string{}

func readevents() {

	file, filerror := os.Open("../tracegen/traces/A600_B40_C1_D1_E300_F200_N3000/events_300000_3000ps.csv")
	if filerror != nil {
		panic(filerror)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	count := 0
	for {

		row, err := reader.Read()
		if count == 0 {
			count += 1
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		newevent := event{row[2], row[3], row[4], row[5], make(chan []endresponse, 1)}
		list := strings.Split(row[6], "|")
		for i, v := range list {
			value, err := strconv.Atoi(v)
			if err != nil {
				panic(err)
			}
			list[i] = endpoints[value].id
		}
		Subscriptions[newevent.id] = list
		i, err := strconv.Atoi(row[1])
		if err != nil {
			panic(err)
		}
		newsender := sender{i, newevent}
		holdingchannel <- newsender
	}

}

func Generate() {

	readevents()
	start := time.Now()

	for range len(holdingchannel) {
		one := <-holdingchannel
		when := start.Add(time.Duration(one.at * int(time.Millisecond)))
		now := time.Now()
		if when.Before(now) {
			EventChannel <- one.event
		} else {
			time.Sleep(time.Until(when))
			EventChannel <- one.event
		}

		//to avoid blocking
		select {
		case <-one.event.response:

		default:

		}

	}
	quit <- syscall.SIGINT

}
