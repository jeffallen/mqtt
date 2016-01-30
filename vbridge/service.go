package main

import (
	"fmt"
	"net"
	"sync"
	"time"

	proto "github.com/huin/mqtt"
	"github.com/jeffallen/mqtt"
	"github.com/jeffallen/mqtt/vbridge/ifc"
	"v.io/v23/context"
)

type impl struct {
}

func makeImpl() ifc.BridgeServerMethods {
	return &impl{}
}

func mqttConnect() (*mqtt.ClientConn, *sync.Mutex, error) {
	conn, err := net.Dial("tcp", *host)
	if err != nil {
		return nil, nil, err
	}
	cc := mqtt.NewClientConn(conn)
	cc.Dump = *dump
	cc.ClientId = *id

	if err := cc.Connect(*user, *pass); err != nil {
		return nil, nil, err
	}
	return cc, &sync.Mutex{}, err
}

func (f *impl) Subscribe(ctx *context.T, sc ifc.BridgeSubscribeServerCall, topics []ifc.Topic) error {
	ctx.Info("Subscribe for topics ", topics)

	cc, _, err := mqttConnect()
	if err != nil {
		return err
	}

	done := make(chan error)

	// Sender
	go func() {
		defer ctx.Info("sender exited")

		suback := cc.Subscribe(tq(topics))

		if len(suback.TopicsQos) != len(topics) {
			done <- txerr(fmt.Errorf("suback has topic list length %v", len(suback.TopicsQos)))
			return
		}

		// Since cc.Incoming is a channel, concurrent access to it is ok.
		for m1 := range cc.Incoming {
			if bp, ok := m1.Payload.(proto.BytesPayload); !ok {
				done <- txerr(fmt.Errorf("payload type %T not handled", m1.Payload))
				return
			} else {
				m2 := ifc.Message{
					Topic:   m1.TopicName,
					Payload: []byte(bp),
				}
				err := sc.SendStream().Send(m2)
				if err != nil {
					done <- txerr(err)
					return
				}
			}
		}
	}()

	// Receiver
	go func() {
		defer ctx.Info("receiver exited")

		remote := sc.RemoteEndpoint()
		ctx.Info("remote: ", remote.Name())

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		done <- rxerr(receive(ctx, remote.Name(), topics))
	}()

	// Wait for either the sender or the receiver to finish.
	err = <-done

	// Now cleanup so that both goroutines are guaranteed to exit.

	// Stop sender by closing cc.Incoming
	cc.Disconnect()

	ctx.Info("done with err = ", err)
	return err
}

func tq(topics []ifc.Topic) []proto.TopicQos {
	tq := make([]proto.TopicQos, len(topics))
	for i, t := range topics {
		tq[i].Topic = string(t)
		tq[i].Qos = proto.QosAtMostOnce
	}
	return tq
}

func rxerr(err error) error {
	return fmt.Errorf("receiver error: %v", err)
}
func txerr(err error) error {
	return fmt.Errorf("sender error: %v", err)
}

func receive(ctx *context.T, remote string, topics []ifc.Topic) error {
	cc, _, err := mqttConnect()
	if err != nil {
		return err
	}

	bc := ifc.BridgeClient(remote)
	bcc, err := bc.Subscribe(ctx, topics)
	if err != nil {
		return err
	}

	rs := bcc.RecvStream()
	for {
		if ok := rs.Advance(); !ok {
			return rs.Err()
		}
		m1 := rs.Value()

		cc.Publish(&proto.Publish{
			Header:    proto.Header{Retain: false},
			TopicName: string(m1.Topic),
			Payload:   proto.BytesPayload(m1.Payload),
		})
	}
}
