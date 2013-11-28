package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"code.google.com/p/jra-go/mqtt"
	"github.com/eclesh/welford"
	proto "github.com/huin/mqtt"
)

var conns = flag.Int("conns", 100, "how many connections")
var wsubs = flag.Int("wsubs", 20, "how many wildcard subscribers")
var messages = flag.Int("messages", 1000, "how many messages")
var host = flag.String("host", "localhost:1883", "hostname of broker")
var dump = flag.Bool("dump", false, "dump messages?")
var id = flag.String("id", "", "client id")
var user = flag.String("user", "", "username")
var pass = flag.String("pass", "", "password")

var cwg sync.WaitGroup

type sd struct {
	mu sync.Mutex
	w  welford.Stats
}

func (s *sd) add(sample time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.w.Add(float64(sample))
}

func (s *sd) calculate() (min, max, mean, stddev time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	min = time.Duration(s.w.Min())
	max = time.Duration(s.w.Max())
	mean = time.Duration(s.w.Mean())
	stddev = time.Duration(s.w.Stddev())

	return
}

var stddev sd

func main() {
	log.SetFlags(log.Lmicroseconds)
	flag.Parse()

	timeStart := time.Now()

	// a system to check how long connection establishment takes
	cwg.Add(*conns + *wsubs)
	go func() {
		cwg.Wait()
		log.Print("all connections made")
	}()

	// start the wildcard subscribers
	wsubWg.Add(*wsubs)
	for i := 0; i < *wsubs; i++ {
		go wsub()
	}

	var wg sync.WaitGroup
	publishers := *conns / 2
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			ping(i)
			wg.Done()
		}(i)
	}
	log.Print("all started")
	wg.Wait()
	log.Print("all finished")

	timeEnd := time.Now()

	log.Print("checking wildcard subscribers:")
	wsubWg.Wait()
	log.Print("ok")

	elapsed := timeEnd.Sub(timeStart)
	totmsg := float64(*messages * 2 * publishers)
	msgpersec := totmsg / float64(elapsed/time.Second)

	log.Print("elapsed time: ", elapsed)
	log.Print("messages    : ", totmsg)
	log.Print("messages/sec: ", msgpersec)

	log.Print("round-trip latency")
	min, max, mean, stddev := stddev.calculate()
	log.Print("min   : ", min)
	log.Print("max   : ", max)
	log.Print("mean  : ", mean)
	log.Print("stddev: ", stddev)
}

func ping(i int) {
	topic := fmt.Sprintf("pingtest/%v/request", i)
	topic2 := fmt.Sprintf("pingtest/%v/reply", i)

	start := make(chan struct{})
	stop := make(chan struct{})

	// the goroutine to reply to pings for this pair
	go func() {
		payload := []byte("ok")
		cc := connect()
		ack := cc.Subscribe([]proto.TopicQos{
			{topic, proto.QosAtLeastOnce},
		})
		if *dump {
			fmt.Printf("suback: %#v\n", ack)
		}

		close(start)
		for {
			select {
			// for each incoming message, send it back unchanged
			case in := <-cc.Incoming:
				if *dump {
					fmt.Printf("request: %#v\n", in)
				}
				in.TopicName = topic2
				in.Payload = proto.BytesPayload(payload)
				cc.Publish(in)
			case _ = <-stop:
				cc.Disconnect()
				return
			}
		}
	}()

	_ = <-start

	cc := connect()

	ack := cc.Subscribe([]proto.TopicQos{
		{topic2, proto.QosAtLeastOnce},
	})
	if *dump {
		fmt.Printf("suback: %#v\n", ack)
	}

	for i := 0; i < *messages; i++ {
		payload := []byte(fmt.Sprintf("ping %v", i))

		timeStart := time.Now()
		cc.Publish(&proto.Publish{
			Header:    proto.Header{QosLevel: proto.QosAtMostOnce},
			TopicName: topic,
			Payload:   proto.BytesPayload(payload),
		})

		in := <-cc.Incoming
		if *dump {
			fmt.Printf("reply: %#v\n", in)
		}

		elapsed := time.Now().Sub(timeStart)
		stddev.add(elapsed)

		buf := &bytes.Buffer{}
		err := in.Payload.WritePayload(buf)
		if err != nil {
			log.Println("payload data:", err)
			panic("ugh")
		}

		if !bytes.Equal(buf.Bytes(), []byte("ok")) {
			log.Println("unexpected reply: ", string(buf.Bytes()))
			break
		}
	}

	cc.Disconnect()
	close(stop)
}

func connect() *mqtt.ClientConn {
	conn, err := net.Dial("tcp", *host)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(2)
	}
	cc := mqtt.NewClientConn(conn)
	cc.Dump = *dump
	cc.ClientId = *id

	err = cc.Connect(*user, *pass)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	cwg.Done()
	return cc
}

var wsubWg sync.WaitGroup

func wsub() {
	topic := "pingtest/1/#"

	cc := connect()
	defer cc.Disconnect()

	ack := cc.Subscribe([]proto.TopicQos{
		{topic, proto.QosAtLeastOnce},
	})
	if *dump {
		fmt.Printf("suback: %#v\n", ack)
	}

	count := 0
	for _ = range cc.Incoming {
		count++
		if count == (2 * *messages) {
			wsubWg.Done()
			return
		}
	}
}
