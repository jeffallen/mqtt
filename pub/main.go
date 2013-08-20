package main

import (
	"fmt"
	"net"
	"os"

	"code.google.com/p/jra-go/mqtt"
	proto "github.com/jeffallen/mqtt"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: pub topic message")
		return
	}

	conn, err := net.Dial("tcp", "127.0.0.1:1883")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: ", err)
		return
	}
	cc := mqtt.NewClientConn(conn)
	cc.Start()

	cc.Connect()
	cc.Publish(&proto.Publish{
		TopicName: os.Args[1],
		Payload:   proto.BytesPayload([]byte(os.Args[2])),
	})
	cc.Disconnect()
}
