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

	// Start serving.
	bsrv := ifc.BridgeServer(makeImpl())
	_, server, err := v23.WithNewServer(ctx, *serviceName, bsrv,
		security.DefaultAuthorizer())

	if err != nil {
		log.Panic("Error serving service: ", err)
	}

	endpoint := server.Status().Endpoints[0]
	fmt.Printf("Listening at: %v\n", endpoint)

	// Initiate bridge?
	if *topics != "" {
		tlist := strings.Split(*topics, ",")

		ifct := make([]ifc.Topic, len(tlist))
		for i, t := range tlist {
			ifct[i] = ifc.Topic(t)
		}

		err := receive(ctx, *to, ifct)
		if err != nil {
			log.Println("initiate error: ", err)
		}
		v23.GetAppCycle(ctx).Stop(ctx)
	}

	// Wait forever.
	<-signals.ShutdownOnSignals(ctx)
}
