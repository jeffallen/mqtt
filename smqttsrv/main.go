package main

import (
	"crypto/tls"
	"github.com/jeffallen/mqtt"
	"log"
)

// See http://mosquitto.org/man/mosquitto-tls-7.html for how to make the
// server.{crt,key} files. Then use mosquitto like this to talk to it:
//
//   mosquitto_sub  --cafile ca.crt --tls-version tlsv1 -p 8883
//
// tls-version is required, because Go's TLS is limited to TLS 1.0, but
// OpenSSL will try to ask for TLS 1.2 by default.

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
		NextProtos:   []string{"mqtt"},
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
