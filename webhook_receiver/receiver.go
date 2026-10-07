package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

var starttime time.Time
var deliverystart atomic.Bool
var numbusinessA int
var numbusinessB int
var numbusinessC int
var numbusinessD int
var numbusinessE int
var numbusinessF int

type profile struct {
	profiletype      int
	id               string
	url              string
	concurrencylimit int
	ratelimit        int
	responsetime     int
	outage           bool
}

func genendpoint(profile profile) http.HandlerFunc {
	merchantchan := make(chan int, profile.concurrencylimit)
	var endp6last atomic.Int64
	endp6last.Store(time.Now().UnixNano())

	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case merchantchan <- 1:
		default:
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode("too many requests")
			return
		}
		time.Sleep(time.Duration(profile.responsetime) * time.Millisecond)
		randomhalf := rand.Intn(100)
		if deliverystart.Load() == false {
			starttime = time.Now()
			deliverystart.Store(true)
		}

		if profile.profiletype == 1 && profile.outage == true {

			if time.Since(starttime) >= 60*time.Second && time.Since(starttime) <= 150*time.Second {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode("internal server error")
				<-merchantchan

			} else if time.Since(starttime) >= 270*time.Second && time.Since(starttime) <= 300*time.Second {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode("internal server error")
				<-merchantchan

			} else {

				if randomhalf < 99 {

					w.WriteHeader(http.StatusAccepted)
					json.NewEncoder(w).Encode("succcess on endpoint")

					<-merchantchan

				}
				if randomhalf == 99 {
					w.WriteHeader(http.StatusInternalServerError)
					json.NewEncoder(w).Encode("internal server error")
					<-merchantchan

				}

			}
		} else if profile.profiletype == 5 {
			if randomhalf < 70 {

				w.WriteHeader(http.StatusAccepted)
				json.NewEncoder(w).Encode("succcess on endpoint ")

				<-merchantchan

			}
			if randomhalf >= 70 {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode("internal server error")
				<-merchantchan

			}

		} else {

			if randomhalf < 99 {
				if profile.profiletype == 6 {
					if time.Since(time.Unix(0, endp6last.Load())) < time.Duration(profile.ratelimit)*time.Millisecond {
						w.WriteHeader(http.StatusTooManyRequests)
						json.NewEncoder(w).Encode("too many requests")
						<-merchantchan

					} else {

						w.WriteHeader(http.StatusAccepted)
						json.NewEncoder(w).Encode("succcess on endpoint 3")
						endp6last.Store(time.Now().UnixNano())
						<-merchantchan

					}
				} else {

					w.WriteHeader(http.StatusAccepted)
					json.NewEncoder(w).Encode("succcess on endpoint")

					<-merchantchan
				}

			}
			if randomhalf == 99 {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode("internal server error")
				<-merchantchan

			}

		}

	}
}

func home(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode("successfully connected to server")
}

func home2(w http.ResponseWriter, r *http.Request) {

	if time.Since(starttime) >= 60*time.Second && time.Since(starttime) <= 150*time.Second {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode("internal server error")

	} else if time.Since(starttime) >= 270*time.Second && time.Since(starttime) <= 300*time.Second {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode("internal server error")

	} else {

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode("succcess on endpoint 2")
	}

}

func main() {

	deliverystart.Store(false)
	mux := http.NewServeMux()
	var NumbusinessA = 600
	var NumbusinessB = 40
	var NumbusinessC = 1
	var NumbusinessD = 1
	var NumbusinessE = 300
	var NumbusinessF = 200
	endpointcount := 0
	numoutages := 1
	outage := false
	for range NumbusinessA {

		url := strconv.Itoa(endpointcount)

		if numoutages > 0 {
			outage = true
			numoutages -= 1
		} else {
			outage = false
		}
		var profile1 = profile{
			profiletype:      1,
			id:               "merchant_backends",
			url:              url,
			concurrencylimit: 10,
			ratelimit:        100,
			responsetime:     100,
			outage:           outage,
		}
		endpoint := genendpoint(profile1)
		mux.HandleFunc("POST /"+profile1.url, endpoint)
		if outage == true {
			mux.HandleFunc("GET /"+profile1.url, home2)
		} else {
			mux.HandleFunc("GET /"+profile1.url, home)
		}
		endpointcount++
	}
	for range NumbusinessB {

		url := strconv.Itoa(endpointcount)
		var profile1 = profile{
			profiletype:      2,
			id:               "on_prem_ERP_connectors",
			url:              url,
			concurrencylimit: 2,
			ratelimit:        5000,
			responsetime:     5000,
		}
		endpoint := genendpoint(profile1)
		mux.HandleFunc("POST /"+profile1.url, endpoint)
		mux.HandleFunc("GET /"+profile1.url, home)
		endpointcount++
	}
	for range NumbusinessC {

		url := strconv.Itoa(endpointcount)
		var profile1 = profile{
			profiletype:      3,
			id:               "fraud_scoring_vendor",
			url:              url,
			concurrencylimit: 500,
			ratelimit:        150,
			responsetime:     150,
		}
		endpoint := genendpoint(profile1)
		mux.HandleFunc("POST /"+profile1.url, endpoint)
		mux.HandleFunc("GET /"+profile1.url, home)
		endpointcount++
	}
	for range NumbusinessD {

		url := strconv.Itoa(endpointcount)
		var profile1 = profile{
			profiletype:      4,
			id:               "internal_analytics_pipeline",
			url:              url,
			concurrencylimit: 1000,
			ratelimit:        50,
			responsetime:     50,
		}
		endpoint := genendpoint(profile1)
		mux.HandleFunc("POST /"+profile1.url, endpoint)
		mux.HandleFunc("GET /"+profile1.url, home)
		endpointcount++
	}
	for range NumbusinessE {

		url := strconv.Itoa(endpointcount)
		var profile1 = profile{
			profiletype:      5,
			id:               "small_merchant_on_shared_hosting",
			url:              url,
			concurrencylimit: 3,
			ratelimit:        800,
			responsetime:     800,
		}
		endpoint := genendpoint(profile1)
		mux.HandleFunc("POST /"+profile1.url, endpoint)
		mux.HandleFunc("GET /"+profile1.url, home)
		endpointcount++
	}
	for range NumbusinessF {

		url := strconv.Itoa(endpointcount)
		var profile1 = profile{
			profiletype:      6,
			id:               "ops_notifiers",
			url:              url,
			concurrencylimit: 1,
			ratelimit:        1000,
			responsetime:     50,
		}
		endpoint := genendpoint(profile1)
		mux.HandleFunc("POST /"+profile1.url, endpoint)
		mux.HandleFunc("GET /"+profile1.url, home)
		endpointcount++
	}

	srv := &http.Server{
		Addr:    ":8080",
		Handler: (mux),
	}
	//internal errors
	serverError := make(chan error, 1)
	go func() {
		serverError <- srv.ListenAndServe()
	}()
	fmt.Println("server is up")
	//external errors
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverError:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
		}

	case <-quit:
		log.Println("shutdown signal received")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}

}
