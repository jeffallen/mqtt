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

func main() {
	flag.Parse()

	var wg sync.WaitGroup
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func() {
			run()
			wg.Done()
		}()
	}
	wg.Wait()
}

func run() {
	conn, err := net.Dial("tcp", "127.0.0.1:1883")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: ", err)
		return
	}
	cc := mqtt.NewClientConn(conn)
	cc.Start()

	cc.Connect()
	for i := 0; i < *messages; i++ {
		cc.Publish(&proto.Publish{
			TopicName: "spam",
			Payload:   proto.BytesPayload([]byte("spam")),
		})
	}

	cc.Disconnect()
}
