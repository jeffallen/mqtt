package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jeffallen/mqtt/vbridge/ifc"

	"v.io/v23"
	"v.io/v23/options"
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

	timeout = flag.Duration("timeout", 5*time.Second, "the timeout for the RPC connection")
	topics  = flag.String("topics", "", "topics to bridge")
	to      = flag.String("to", "", "Vanadium service name for other side of the bridge")
	from    = flag.String("from", "", "Enable pipe mode: Vanadium service name for other side of the pipe")
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
	} else if *to != "" && *from != "" {
		// pipe mode
		ifct := ifcTopics(*topics)
		tmout := options.ChannelTimeout(*timeout)

		leftc := ifc.BridgeClient(*to)
		rightc := ifc.BridgeClient(*from)

		leftcc, err := leftc.Link(ctx, ifct, tmout)
		if err != nil {
			ctx.Error(err)
			return
		}

		rightcc, err := rightc.Link(ctx, ifct, tmout)
		if err != nil {
			ctx.Error(err)
			return
		}

		errCh := make(chan error, 2)

		wg := &sync.WaitGroup{}
		wg.Add(2)
		go linkToLink(leftcc.RecvStream(), rightcc.SendStream(), errCh, wg)
		go linkToLink(rightcc.RecvStream(), leftcc.SendStream(), errCh, wg)
		wg.Wait()
		select {
		case err := <-errCh:
			log.Print("pipe error: ", err)
		default:
			// don't block on channel read
		}
	} else {
		cc, mu, err := mqttConnect()
		if err != nil {
			ctx.Error("mqtt connect: ", err)
			return
		}

		bc := ifc.BridgeClient(*to)

		ifct := ifcTopics(*topics)
		bcc, err := bc.Link(ctx, ifct, options.ChannelTimeout(*timeout))
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

func ifcTopics(topics string) (ifct []ifc.Topic) {
	tlist := strings.Split(topics, ",")
	ifct = make([]ifc.Topic, len(tlist))
	for i, t := range tlist {
		ifct[i] = ifc.Topic(t)
	}
	return
}

func linkToLink(rs rs, ss ss, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if ok := rs.Advance(); !ok {
			errCh <- rs.Err()
			return
		}
		m := rs.Value()
		if err := ss.Send(m); err != nil {
			errCh <- err
			return
		}
	}
}
