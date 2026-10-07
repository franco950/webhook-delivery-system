package main

import (
	"fmt"
	"io"
	"math/rand"
	"time"
)

func (endpoint *endpoint) healthcheck(health chan bool) {

	for range health {
		resp, _ := client.Get(endpoint.url)
		if resp != nil {
			_, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Println(err)
			}

			resp.Body.Close()
			if resp.StatusCode < 300 {
				endpoint.consecfailures.Store(0)
				endpoint.probeactive.Store(false)

				close(health)
				return

			}
		}

		time.Sleep(10 * time.Second)
		health <- false
	}
}
func (endpoint *endpoint) probe() {
	if endpoint.probeactive.Load() == true {
		return
	}
	endpoint.probeactive.Store(true)
	health := make(chan bool, 1)
	health <- false
	go endpoint.healthcheck(health)
}
func (endpoint *endpoint) backoff(current *delivery) {

	exponential := configs.baseduration * time.Duration(1<<uint(len(current.attempts)))
	if exponential > configs.maxduration {
		exponential = configs.maxduration
	}
	jitter := time.Duration(rand.Intn(int(configs.baseduration)))
	backoff := exponential + jitter

	current.retryAt = time.Now().Add(backoff)
	current.trydone.Store(false)

}
func (endpoint *endpoint) getresources() []*delivery {

	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	deliverylist := []*delivery{}

	for i, _ := range endpoint.resourcemap {

		delivery := endpoint.resourcemap[i][0]
		if len(delivery.attempts) == 0 {
			continue
		} else if len(delivery.attempts) > 0 && len(delivery.attempts) >= configs.maxattempts && (delivery.attempts[len(delivery.attempts)-1]).status >= 300 {

			if len(endpoint.resourcemap[delivery.resourceID]) > 1 {
				next := endpoint.resourcemap[i][1]

				deliverylist = append(deliverylist, next)
				endpoint.resourcemap[delivery.resourceID] = endpoint.resourcemap[delivery.resourceID][1:]
			} else {
				delete(endpoint.resourcemap, delivery.resourceID)
			}

		} else if len(delivery.attempts) > 0 && (delivery.retryAt).Before(time.Now()) && (delivery.attempts[len(delivery.attempts)-1]).status >= 300 && delivery.trydone.Load() == false {

			deliverylist = append(deliverylist, delivery)
			delivery.trydone.Store(true)

		} else if len(delivery.attempts) > 0 && (delivery.attempts[len(delivery.attempts)-1]).status < 300 {

			if len(endpoint.resourcemap[delivery.resourceID]) > 1 {
				next := endpoint.resourcemap[i][1]
				deliverylist = append(deliverylist, next)
				endpoint.resourcemap[delivery.resourceID] = endpoint.resourcemap[delivery.resourceID][1:]
			} else {
				delete(endpoint.resourcemap, delivery.resourceID)
			}

		}
	}
	return deliverylist
}
func (endpoint *endpoint) retrywork() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if endpoint.resizecalled.Load() == true {
			resizedone := endpoint.resizedone
			if endpoint.resizeongoing.Load() == false {

				endpoint.resizeack <- 1

			}
			<-resizedone
		}

		resources := endpoint.getresources()

		for i := 0; i < len(resources); i++ {

			laneresource := endpoint.laneresources[endpoint.getlaneid(resources[i])]
			select {
			case laneresource <- resources[i]:
			case <-endpoint.resizewake:
				if endpoint.resizecalled.Load() == true && endpoint.resizeongoing.Load() == false {
					resizedone := endpoint.resizedone
					endpoint.resizeack <- 1
					<-resizedone
				}
				i -= 1
			}

		}

		select {
		case <-endpoint.resizewake:
		case <-ticker.C:

		}

	}

}
