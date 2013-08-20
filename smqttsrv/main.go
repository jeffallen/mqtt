package main

import (
	"code.google.com/p/jra-go/mqtt"
	"crypto/tls"
	"log"
)

func readCert() []tls.Certificate {
	c, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		panic(err)
	}
	return []tls.Certificate{c}
}

func main() {
	cfg := &tls.Config{
		Certificates: readCert(),
		//NextProtos:   []string{"mqtt"},
	}
	l, err := tls.Listen("tcp", ":8883", cfg)
	if err != nil {
		log.Print("listen: ", err)
		return
	}
	svr := mqtt.NewServer(l)
	svr.Start()
	<-svr.Done
}
