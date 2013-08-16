package main

import (
	"code.google.com/p/jra-go/mqtt"
	"log"
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":1883")
	if err != nil {
		log.Print("listen: ", err)
		return
	}
	svr := mqtt.NewServer(l)
	<-svr.Done
}
