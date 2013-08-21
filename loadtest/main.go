package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"

	"code.google.com/p/jra-go/mqtt"
	proto "github.com/jeffallen/mqtt"
)

var conns = flag.Int("conns", 100, "how many connections")
var messages = flag.Int("messages", 100, "how many messages")
var host = flag.String("host", "localhost:1883", "hostname of broker")
var dump = flag.Bool("dump", false, "dump messages?")
var id = flag.String("id", "", "client id")
var user = flag.String("user", "", "username")
var pass = flag.String("pass", "", "password")

func main() {
	flag.Parse()

	var wg sync.WaitGroup
	for i := 0; i < *conns; i++ {
		wg.Add(2)
		go func(i int) {
			sub(i, &wg)
			pub(i)
			wg.Done()
		}(i)
	}
	println("all started")
	wg.Wait()
	println("all finished")

	rc := 0
loop:
	for {
		select {
		case i := <-bad:
			println("subscriber missed messages: ", i)
			rc = 1
		default:
			// nothing to read on bad, done.
			break loop
		}
	}
	os.Exit(rc)
}

func pub(i int) {
	topic := fmt.Sprintf("loadtest/%v", i)

	var cc *mqtt.ClientConn
	if cc = connect(); cc == nil {
		return
	}

	for i := 0; i < *messages; i++ {
		cc.Publish(&proto.Publish{
			Header:    proto.Header{QosLevel: proto.QosAtLeastOnce},
			TopicName: topic,
			Payload:   proto.BytesPayload([]byte("loadtest payload")),
		})
	}

	cc.Disconnect()
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
		return nil
	}
	return cc
}

// A channel to communicate subscribers that didn't get what they expected
var bad = make(chan int)

func sub(i int, wg *sync.WaitGroup) {
	topic := fmt.Sprintf("loadtest/%v", i)

	var cc *mqtt.ClientConn
	if cc = connect(); cc == nil {
		return
	}

	ack := cc.Subscribe([]proto.TopicQos{
		{topic, proto.QosAtLeastOnce},
	})
	if *dump {
		fmt.Printf("suback: %#v\n", ack)
	}

	go func() {
		ok := false
		count := 0
		for _ = range cc.Incoming {
			count++
			println("count", count)
			if count == *messages {
				cc.Disconnect()
				ok = true
			}
		}

		if !ok {
			bad <- i
		}

		wg.Done()
	}()
}
