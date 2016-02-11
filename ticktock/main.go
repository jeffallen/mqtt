package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	proto "github.com/huin/mqtt"
	"github.com/jeffallen/mqtt"
)

var host = flag.String("host", "localhost:1883", "hostname of broker")
var id = flag.String("id", "", "client id")
var user = flag.String("user", "", "username")
var pass = flag.String("pass", "", "password")
var dump = flag.Bool("dump", false, "dump messages?")
var delay = flag.Duration("delay", time.Second, "delay between messages")
var who = flag.String("who", "bonnie", "who is this? (to make two instance of ticktock distinct)")

func main() {
	flag.Parse()

	conn, err := net.Dial("tcp", *host)
	if err != nil {
		fmt.Fprint(os.Stderr, "dial: ", err)
		return
	}
	cc := mqtt.NewClientConn(conn)
	cc.Dump = *dump
	cc.ClientId = *id

	tq := []proto.TopicQos{
		{Topic: "tick", Qos: proto.QosAtMostOnce},
	}

	if err := cc.Connect(*user, *pass); err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}

	cc.Subscribe(tq)

	// Sender
	go func() {
		for {
			now := time.Now()
			what := fmt.Sprintf("%v at %v", *who, now)

			cc.Publish(&proto.Publish{
				Header:    proto.Header{Retain: false},
				TopicName: "tick",
				Payload:   proto.BytesPayload([]byte(what)),
			})

			time.Sleep(*delay)
		}
	}()

	// Receiver
	for m := range cc.Incoming {
		fmt.Print(m.TopicName, "\t")
		m.Payload.WritePayload(os.Stdout)
		fmt.Println("\tr: ", m.Header.Retain)
	}
}
