package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/maphash"
	"io"
	"path"
	"time"
)

func (endpoint *endpoint) lane(lane, laneresource chan *delivery) {

	var lastrelease time.Time = time.Now()

	for {
		if endpoint.resizecalled.Load() == true {
			resizedone := endpoint.resizedone
			if endpoint.resizeongoing.Load() == false {

				endpoint.resizeack <- 1

			}
			<-resizedone
		}
		var work *delivery

		select {
		case <-endpoint.resizewake:
			if endpoint.resizecalled.Load() == true {
				resizedone := endpoint.resizedone
				if endpoint.resizeongoing.Load() == false {

					endpoint.resizeack <- 1

				}
				<-resizedone
			}
			continue

		case work = <-laneresource:
		case work = <-lane:

		}

		endpoint.mu.Lock()
		resource, ok := endpoint.resourcemap[work.resourceID]

		if ok == true && work != resource[0] {
			endpoint.resourcemap[work.resourceID] = append(resource, work)
			work = nil

		}

		endpoint.mu.Unlock()

		if work == nil {
			continue
		}
		endpoint.holding.Add(1)
		_ = <-endpoint.tokenchan

		lastrelease = endpoint.deliver(work, lastrelease)
		endpoint.holding.Add(-1)
		endpoint.mu.Lock()
		if endpoint.giventokens.Load() <= endpoint.tokencount.Load() {
			endpoint.tokenchan <- 1

		} else {
			endpoint.giventokens.Add(-1)
		}
		endpoint.mu.Unlock()
	}

}
func (endpoint *endpoint) checkpace() {

	for _, x := range endpoint.history {
		if x.reducecalled == true {

			return
		}
	}
	length := len(endpoint.history)
	if length > 1 {
		last := endpoint.history[length-1]
		before := endpoint.history[length-2]

		if last.response < 300 && endpoint.inflightcount.Load()+int64(1) >= int64(endpoint.tokencount.Load()) {
			if before.response < 300 {
				if endpoint.tokencount.Load() < endpoint.lanecount.Load() {
					endpoint.tokencount.Add(1)

					if endpoint.tokencount.Load() > endpoint.giventokens.Load() {
						endpoint.tokenchan <- 1
						endpoint.giventokens.Add(1)

					}
				}

			}

		}
		if last.response < 300 && time.Duration(endpoint.ratelimit.Load())*(time.Millisecond) > last.attemptduration {
			if before.response < 300 && before.reducecalled == false && endpoint.ratelimit.Load() > int32(configs.checkpacetime) {
				endpoint.ratelimit.Add(-int32(configs.checkpacetime))
			}
		}

	}
}
func (endpoint *endpoint) reducepace() {

	if endpoint.tokencount.Load() > 1 {
		endpoint.tokencount.Add(-1)

	} else {

		endpoint.ratelimit.Add(int32(configs.reducepacetime))

	}

}
func (endpoint *endpoint) resizelane() {

	draining := 1
	select {
	case draining = <-draincalled:
	default:
	}
	if int32(cap(endpoint.tokenchan)) == endpoint.lanecount.Load() || endpoint.probeactive.Load() == true || draining == 0 {
		return
	}
	endpoint.resizedone = make(chan int)
	endpoint.resizecalled.Store(true)
	close(endpoint.resizewake)
	for range endpoint.lanecount.Load() + 2 {
		<-endpoint.resizeack
	}
	endpoint.resizeongoing.Store(true)
	var resizechan = make(chan *delivery, endpoint.lanecount.Load()*int32(configs.endpointcap))
	if endpoint.tokencount.Load() == endpoint.lanecount.Load() {
		y := endpoint.lanecount.Load()
		var count int
		if endpoint.lanecount.Load()+y > int32(cap(endpoint.tokenchan)) {
			y = int32(cap(endpoint.tokenchan)) - endpoint.lanecount.Load()
			count = int(endpoint.lanecount.Load())
		} else {
			count = int(y)
		}

		endpoint.lanecount.Add(y)

		for range y {
			endpoint.lanemap[count] = make(chan *delivery, configs.endpointcap)
			endpoint.laneresources[count] = make(chan *delivery, configs.retrychansize)
			go endpoint.lane(endpoint.lanemap[count], endpoint.laneresources[count])
			count += 1
		}

		for _, v := range endpoint.laneresources {
			for len(v) > 0 {
				resizechan <- <-v

			}

		}
		for len(resizechan) > 0 {
			delivery := <-resizechan
			id := endpoint.getlaneid(delivery)
			endpoint.laneresources[id] <- delivery

		}
		for _, v := range endpoint.lanemap {
			for len(v) > 0 {
				resizechan <- <-v

			}

		}
		for len(resizechan) > 0 {
			delivery := <-resizechan
			id := endpoint.getlaneid(delivery)
			endpoint.lanemap[id] <- delivery

		}

	}

	endpoint.resizewake = make(chan int)

	endpoint.resizecalled.Store(false)

	endpoint.resizeongoing.Store(false)
	close(endpoint.resizedone)

}
func (endpoint *endpoint) lanehold() {
	for endpoint.consecfailures.Load() >= int32(configs.fuse) {
		endpoint.probe()
		time.Sleep(1 * time.Second)

	}
}

func (endpoint *endpoint) lanework() {

	for id, lane := range endpoint.lanemap {
		laneresource := endpoint.laneresources[id]
		go endpoint.lane(lane, laneresource)

	}

	for delivery := range endpoint.queue {

		if endpoint.lanecount.Load() == endpoint.tokencount.Load() {
			endpoint.resizelane()
		}
		laneid := endpoint.getlaneid(delivery)
		endpoint.lanemap[int(laneid)] <- delivery

	}

}
func (endpoint *endpoint) deliver(current *delivery, lastrelease time.Time) time.Time {
	endpoint.lanehold()

	duration := time.Duration(endpoint.ratelimit.Load()) * (time.Millisecond)
	sincerelease := time.Since(lastrelease)
	if duration > sincerelease {
		time.Sleep(duration - sincerelease)
	}
	lastrelease = time.Now()

	parking := 0
	start := time.Now()
	newpayload := payload{current.id, current.resourceID, current.message, current.eventid}
	data, jsonerr := json.Marshal(newpayload)
	if jsonerr != nil {
		panic(jsonerr)
	}
	endpoint.inflightcount.Add(1)
	resp, err := client.Post(endpoint.url, "application/json", bytes.NewBuffer(data))
	endpoint.inflightcount.Add(-1)

	if resp != nil {

		_, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			fmt.Println(err)
		}

		resp.Body.Close()

	}
	if resp != nil && resp.StatusCode < 300 {

		if endpoint.consecfailures.Load() > 0 {
			endpoint.consecfailures.Store(0)
		}
		duration := time.Since(start)

		endpoint.mu.Lock()

		endpoint.history = append(endpoint.history, history{time.Now(), endpoint.inflightcount.Load(), resp.StatusCode, duration, false})
		if len(endpoint.history) > configs.historylength {
			endpoint.history = endpoint.history[1:]
		}
		endpoint.checkpace()

		endpoint.mu.Unlock()
		var att = attempt{time.Now(), err, resp.StatusCode, duration}
		current.attempts = append(current.attempts, att)

		row := metric{current.id, current.eventid, current.eventbornat, endpoint.id, path.Base(endpoint.url), current.message, current.resourceID, att.timestamp, att.err, att.status, att.duration, current.retryAt, parking}
		metrics <- row

		return lastrelease

	}

	if resp == nil || err != nil || resp.StatusCode >= 400 {
		var result int

		if resp == nil {

			result = 0
		} else {

			result = resp.StatusCode
		}
		duration := time.Since(start)
		endpoint.mu.Lock()
		if result == 429 {

			endpoint.history = append(endpoint.history, history{time.Now(), endpoint.inflightcount.Load(), result, duration, true})

			endpoint.reducepace()
		} else {
			endpoint.history = append(endpoint.history, history{time.Now(), endpoint.inflightcount.Load(), result, duration, false})
		}

		if len(endpoint.history) > configs.historylength {
			endpoint.history = endpoint.history[1:]
		}
		endpoint.mu.Unlock()

		if result != 429 {
			endpoint.consecfailures.Add(1)

		}

		var att = attempt{time.Now(), err, result, duration}
		current.attempts = append(current.attempts, att)

		if len(current.attempts) < configs.maxattempts {
			endpoint.mu.Lock()
			endpoint.backoff(current)
			if len(endpoint.resourcemap[current.resourceID]) == 0 {
				endpoint.resourcemap[current.resourceID] = append(endpoint.resourcemap[current.resourceID], current)
			}
			endpoint.mu.Unlock()

		} else {
			if len(current.attempts) >= configs.maxattempts {
				endpoint.mu.Lock()

				endpoint.parked = append(endpoint.parked, current)

				parking = len(endpoint.parked)
				endpoint.mu.Unlock()

			}
		}
		row := metric{current.id, current.eventid, current.eventbornat, endpoint.id, path.Base(endpoint.url), current.message, current.resourceID, att.timestamp, att.err, att.status, att.duration, current.retryAt, parking}
		metrics <- row

		return lastrelease

	} else {
		duration := time.Since(start)

		endpoint.mu.Lock()
		endpoint.history = append(endpoint.history, history{time.Now(), endpoint.inflightcount.Load(), resp.StatusCode, duration, false})
		if len(endpoint.history) > configs.historylength {
			endpoint.history = endpoint.history[1:]
		}

		var att = attempt{time.Now(), err, resp.StatusCode, duration}
		current.attempts = append(current.attempts, att)

		endpoint.parked = append(endpoint.parked, current)
		parking = len(endpoint.parked)
		endpoint.mu.Unlock()

		row := metric{current.id, current.eventid, current.eventbornat, endpoint.id, path.Base(endpoint.url), current.message, current.resourceID, att.timestamp, att.err, att.status, att.duration, current.retryAt, parking}
		metrics <- row
		return lastrelease
	}

}

func (endpoint *endpoint) getlaneid(delivery *delivery) int {
	h := maphash.String(seed, delivery.resourceID)
	laneid := h % uint64(endpoint.lanecount.Load())
	return int(laneid)
}
