package main

import (
	"fmt"
	"net"
	"sync"

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

func (f *impl) Link(ctx *context.T, sc ifc.BridgeLinkServerCall, topics []ifc.Topic) error {
	ctx.Info("Link for topics ", topics)

	done := make(chan error, 2)

	cc, mu, err := mqttConnect()
	if err != nil {
		return err
	}

	// transmitter: do mqtt subscribe, then copy messages to output stream
	go func() {
		done <- transmitter(topics, sc.SendStream(), cc, mu)
	}()

	// receiver: read the input stream and copy to mqtt via cc.Publish
	go func() {
		done <- receiver(sc.RecvStream(), cc, mu)
	}()

	err = <-done

	// Stop sender by closing cc.Incoming
	cc.Disconnect()

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

func txerr(err error) error {
	return fmt.Errorf("tx err: %v", err)
}

func rxerr(err error) error {
	return fmt.Errorf("rx err: %v", err)
}

type rs interface {
	Advance() bool
	Value() ifc.Message
	Err() error
}

func receiver(rs rs, cc *mqtt.ClientConn, mu *sync.Mutex) error {
	for {
		if ok := rs.Advance(); !ok {
			return rxerr(rs.Err())
		}
		m1 := rs.Value()

		mu.Lock()
		cc.Publish(&proto.Publish{
			Header:    proto.Header{Retain: false},
			TopicName: string(m1.Topic),
			Payload:   proto.BytesPayload(m1.Payload),
		})
		mu.Unlock()
	}
}

type ss interface {
	Send(item ifc.Message) error
}

func transmitter(topics []ifc.Topic, ss ss, cc *mqtt.ClientConn, mu *sync.Mutex) error {
	mu.Lock()
	suback := cc.Subscribe(tq(topics))
	mu.Unlock()

	if len(suback.TopicsQos) != len(topics) {
		return txerr(fmt.Errorf("suback has topic list length %v", len(suback.TopicsQos)))
	}

	// Since cc.Incoming is a channel, concurrent access to it is ok.
	for m1 := range cc.Incoming {
		if bp, ok := m1.Payload.(proto.BytesPayload); !ok {
			return txerr(fmt.Errorf("payload type %T not handled", m1.Payload))
		} else {
			m2 := ifc.Message{
				Topic:   m1.TopicName,
				Payload: []byte(bp),
			}
			err := ss.Send(m2)
			if err != nil {
				return txerr(err)
			}
		}
	}
	return nil
}
