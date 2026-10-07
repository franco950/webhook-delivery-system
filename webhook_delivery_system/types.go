package main

import (
	"hash/maphash"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// endpoint id to endpoint
var endpointmap = map[string]*endpoint{}
var endpoints = []*endpoint{}
var seed = maphash.MakeSeed()
var quit = make(chan os.Signal, 1)
var Numendpoints = 1142
var tr, _ = http.DefaultTransport.(*http.Transport)
var custom = tr.Clone()
var client = &http.Client{Transport: custom, Timeout: 10 * time.Second}
var logsdone = make(chan int, 4)
var draincalled = make(chan int)

type configuration struct {
	endpointcap    int           //size of the endpoint queue channel
	maxattempts    int           // maximum number of attempts a delivery is allowed
	baseduration   time.Duration //for backoff
	maxduration    time.Duration //for backoff
	lanecount      int32         // inital number of lanes for endpoints
	tokencount     int           //the concurrency limit ->each lane needs a token to send deliveries
	fuse           int           //maximum number of consecutive failed deliveries allowed
	initratelimit  int
	historylength  int
	tokencap       int //maximum concurrency growth limit
	retrychansize  int
	reducepacetime int
	checkpacetime  int
}

// set via trial and error
var configs = configuration{
	endpointcap:    200,
	maxattempts:    6,
	baseduration:   1 * time.Second,
	maxduration:    60 * time.Second,
	lanecount:      5,
	tokencount:     3,
	retrychansize:  100,
	fuse:           4,
	initratelimit:  100,
	historylength:  10,
	tokencap:       500,
	reducepacetime: 100,
	checkpacetime:  10,
}

type endpoint struct {
	mu             sync.Mutex
	id             string
	lanemap        map[int](chan *delivery)
	queue          chan *delivery
	url            string
	parked         []*delivery
	lanecount      atomic.Int32
	consecfailures atomic.Int32
	ratelimit      atomic.Int32
	probeactive    atomic.Bool
	inflightcount  atomic.Int64
	tokenchan      chan int32
	tokencount     atomic.Int32
	giventokens    atomic.Int32
	history        []history
	holding        atomic.Int32
	resourcemap    map[string][]*delivery
	laneresources  map[int](chan *delivery)
	resizecalled   atomic.Bool
	resizeongoing  atomic.Bool
	resizedone     chan int
	resizeack      chan int
	resizewake     chan int
}
type history struct {
	moment          time.Time
	inflightcount   int64
	response        int
	attemptduration time.Duration
	reducecalled    bool
}

type event struct {
	id         string
	merchantID string
	resourceID string
	message    string
	response   chan []endresponse
}
type attempt struct {
	timestamp time.Time
	err       error
	status    int
	duration  time.Duration
}
type payload struct {
	Id         string
	ResourceId string
	Message    string
	Eventid    string
}

type delivery struct {
	id          string
	eventbornat time.Time
	resourceID  string
	message     string
	eventid     string
	attempts    []attempt
	retryAt     time.Time
	trydone     atomic.Bool
}
