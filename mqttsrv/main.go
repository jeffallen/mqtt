package main

import (
	"code.google.com/p/jra-go/mqtt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
)

type mlRes struct {
	conn net.Conn
	err  error
}

type multiListener struct {
	listeners []net.Listener
	next      chan mlRes
}

func newMultiListener(l ...net.Listener) *multiListener {
	ml := &multiListener{
		listeners: make([]net.Listener, len(l)),
		next:      make(chan mlRes, len(l)),
	}
	copy(ml.listeners, l)

	for _, l := range ml.listeners {
		go func(l net.Listener, next chan mlRes) {
			for {
				res := mlRes{}
				res.conn, res.err = l.Accept()
				next <- res
			}
		}(l, ml.next)
	}

	return ml
}

func (ml *multiListener) Accept() (net.Conn, error) {
	res := <-ml.next
	return res.conn, res.err
}

func (ml *multiListener) Close() error {
	for _, l := range ml.listeners {
		l.Close()
	}
	return nil
}

func (ml *multiListener) Addr() net.Addr {
	return ml.listeners[0].Addr()
}

func main() {
	// see godoc net/http/pprof
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	l, err := net.Listen("tcp", ":1883")
	if err != nil {
		log.Print("listen: ", err)
		return
	}
	svr := mqtt.NewServer(l)
	svr.Start()
	<-svr.Done
}
