package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/jeffallen/mqtt/vbridge/ifc"

	"v.io/v23"
	"v.io/v23/security"
	"v.io/x/ref/lib/signals"
	_ "v.io/x/ref/runtime/factories/generic"
)

var (
	serviceName = flag.String(
		"service-name", "",
		"Name for service in default mount table.")
	host = flag.String("host", "localhost:1883", "hostname of broker")
	id   = flag.String("id", "", "client id")
	user = flag.String("user", "", "username")
	pass = flag.String("pass", "", "password")
	dump = flag.Bool("dump", false, "dump messages?")

	topics = flag.String("topics", "", "topics to bridge")
	to     = flag.String("to", "", "Vanadium service name for other side of the bridge")
)

func main() {
	ctx, shutdown := v23.Init()
	defer shutdown()

	// Run server
	if *to == "" {
		// We are not the caller, so make the RPC available for the
		// caller to call in on.
		bsrv := ifc.BridgeServer(makeImpl())
		_, server, err := v23.WithNewServer(ctx, *serviceName, bsrv,
			security.DefaultAuthorizer())

		if err != nil {
			ctx.Error("Error serving service: ", err)
			return
		}

		endpoint := server.Status().Endpoints[0]
		fmt.Printf("Listening at: %v\n", endpoint)

		// Wait forever.
		<-signals.ShutdownOnSignals(ctx)
	} else {
		cc, mu, err := mqttConnect()
		if err != nil {
			ctx.Error("mqtt connect: ", err)
			return
		}

		tlist := strings.Split(*topics, ",")
		ifct := make([]ifc.Topic, len(tlist))
		for i, t := range tlist {
			ifct[i] = ifc.Topic(t)
		}

		bc := ifc.BridgeClient(*to)
//		timeout := options.ChannelTimeout(2 * time.Second)
		bcc, err := bc.Link(ctx, ifct)
		if err != nil {
			ctx.Error(err)
			return
		}

		done := make(chan error, 2)
		go func() {
			done <- transmitter(ifct, bcc.SendStream(), cc, mu)
			println("send done")
		}()
		go func() {
			done <- receiver(bcc.RecvStream(), cc, mu)
			println("recv done")
		}()
		err = <-done
		log.Print("Stopped with error ", err)

		// Stop sender by closing cc.Incoming
		cc.Disconnect()
	}
}
