package main

import (
	"path"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func (endpoint *endpoint) burstable() bool {
	//I may add more conditions here
	if len(endpoint.queue) < configs.endpointcap {
		return true
	}
	return false
}

func ReceiveEvents(eventchannel <-chan event, done chan struct{}) {

	for eventData := range eventchannel {
		bornat := time.Now()
		id := uuid.New()
		newid := id.String()
		var endresp []endresponse

		for _, v := range Subscriptions[eventData.id] {

			x := endpointmap[v]
			newdelivery := &delivery{newid, bornat, eventData.resourceID, eventData.message, eventData.id, []attempt{}, time.Time{}, atomic.Bool{}}

			if len(x.queue) < configs.endpointcap/2 {

				x.queue <- newdelivery

				endresp = append(endresp, endresponse{x.url: "success"})

				newlog := responselog{time.Now(), eventData.id, path.Base(x.url), "admitted"}
				responsechan <- newlog
				continue
			}

			if len(x.queue) >= configs.endpointcap/2 && x.burstable() == true {
				x.queue <- newdelivery
				endresp = append(endresp, endresponse{x.url: "success"})
				newlog := responselog{time.Now(), eventData.id, path.Base(x.url), "admitted"}
				responsechan <- newlog

				continue

			}

			endresp = append(endresp, endresponse{x.url: "failed"})

			newlog := responselog{time.Now(), eventData.id, path.Base(x.url), "failed"}
			responsechan <- newlog

		}

		eventData.response <- endresp

		select {
		case <-done:
			return
		default:
			continue
		}

	}

}
