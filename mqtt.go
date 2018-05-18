// Package mqtt implements MQTT clients and servers.
package mqtt

import (
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/huin/mqtt"
)

// A random number generator ready to make client-id's, if
// they do not provide them to us.
var cliRand *rand.Rand

func init() {
	var seed int64
	var sb [4]byte
	crand.Read(sb[:])
	seed = int64(time.Now().Nanosecond())<<32 |
		int64(sb[0])<<24 | int64(sb[1])<<16 |
		int64(sb[2])<<8 | int64(sb[3])
	cliRand = rand.New(rand.NewSource(seed))
}

type stats struct {
	recv       int64
	sent       int64
	clients    int64
	clientsMax int64
	lastmsgs   int64
}

func (s *stats) messageRecv()      { atomic.AddInt64(&s.recv, 1) }
func (s *stats) messageSend()      { atomic.AddInt64(&s.sent, 1) }
func (s *stats) clientConnect()    { atomic.AddInt64(&s.clients, 1) }
func (s *stats) clientDisconnect() { atomic.AddInt64(&s.clients, -1) }

func statsMessage(topic string, stat int64) *proto.Publish {
	return &proto.Publish{
		Header:    header(dupFalse, proto.QosAtMostOnce, retainTrue),
		TopicName: topic,
		Payload:   newIntPayload(stat),
	}
}

func (s *stats) publish(sub *subscriptions, interval time.Duration) {
	clients := atomic.LoadInt64(&s.clients)
	clientsMax := atomic.LoadInt64(&s.clientsMax)
	if clients > clientsMax {
		clientsMax = clients
		atomic.StoreInt64(&s.clientsMax, clientsMax)
	}
	sub.submit(nil, statsMessage("$SYS/broker/clients/active", clients))
	sub.submit(nil, statsMessage("$SYS/broker/clients/maximum", clientsMax))
	sub.submit(nil, statsMessage("$SYS/broker/messages/received",
		atomic.LoadInt64(&s.recv)))
	sub.submit(nil, statsMessage("$SYS/broker/messages/sent",
		atomic.LoadInt64(&s.sent)))

	msgs := atomic.LoadInt64(&s.recv) + atomic.LoadInt64(&s.sent)
	msgpersec := (msgs - s.lastmsgs) / int64(interval/time.Second)
	// no need for atomic because we are the only reader/writer of it
	s.lastmsgs = msgs

	sub.submit(nil, statsMessage("$SYS/broker/messages/per-sec", msgpersec))
}

// An intPayload implements proto.Payload, and is an int64 that
// formats itself and then prints itself into the payload.
type intPayload string

func newIntPayload(i int64) intPayload {
	return intPayload(fmt.Sprint(i))
}
func (ip intPayload) ReadPayload(r io.Reader) error {
	// not implemented
	return nil
}
func (ip intPayload) WritePayload(w io.Writer) error {
	_, err := w.Write([]byte(string(ip)))
	return err
}
func (ip intPayload) Size() int {
	return len(ip)
}

// A retain holds information necessary to correctly manage retained
// messages.
//
// This needs to hold copies of the proto.Publish, not pointers to
// it, or else we can send out one with the wrong retain flag.
type retain struct {
	m    proto.Publish
	wild wild
}

type subscriptions struct {
	workers int
	posts   chan post

	mu        sync.Mutex // guards access to fields below
	subs      map[string][]*incomingConn
	wildcards []wild
	retain    map[string]retain
	stats     *stats
}

// The length of the queue that subscription processing
// workers are taking from.
const postQueue = 100

func newSubscriptions(workers int) *subscriptions {
	s := &subscriptions{
		subs:    make(map[string][]*incomingConn),
		retain:  make(map[string]retain),
		posts:   make(chan post, postQueue),
		workers: workers,
	}
	for i := 0; i < s.workers; i++ {
		go s.run(i)
	}
	return s
}

func (s *subscriptions) sendRetain(topic string, c *incomingConn) {
	s.mu.Lock()
	var tlist []string
	if isWildcard(topic) {

		// TODO: select matching topics from the retain map
	} else {
		tlist = []string{topic}
	}
	for _, t := range tlist {
		if r, ok := s.retain[t]; ok {
			c.submit(&r.m)
		}
	}
	s.mu.Unlock()
}

func (s *subscriptions) add(topic string, c *incomingConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if isWildcard(topic) {
		w := newWild(topic, c)
		if w.valid() {
			s.wildcards = append(s.wildcards, w)
		}
	} else {
		s.subs[topic] = append(s.subs[topic], c)
	}
}

type wild struct {
	wild []string
	c    *incomingConn
}

func newWild(topic string, c *incomingConn) wild {
	return wild{wild: strings.Split(topic, "/"), c: c}
}

func (w wild) matches(parts []string) bool {
	i := 0
	for i < len(parts) {
		// topic is longer, no match
		if i >= len(w.wild) {
			return false
		}
		// matched up to here, and now the wildcard says "all others will match"
		if w.wild[i] == "#" {
			return true
		}
		// text does not match, and there wasn't a + to excuse it
		if parts[i] != w.wild[i] && w.wild[i] != "+" {
			return false
		}
		i++
	}

	// make finance/stock/ibm/# match finance/stock/ibm
	if i == len(w.wild)-1 && w.wild[len(w.wild)-1] == "#" {
		return true
	}

	if i == len(w.wild) {
		return true
	}
	return false
}

// Find all connections that are subscribed to this topic.
func (s *subscriptions) subscribers(topic string) []*incomingConn {
	s.mu.Lock()
	defer s.mu.Unlock()

	// non-wildcard subscribers
	res := s.subs[topic]

	// process wildcards
	parts := strings.Split(topic, "/")
	for _, w := range s.wildcards {
		if w.matches(parts) {
			res = append(res, w.c)
		}
	}

	return res
}

// Remove all subscriptions that refer to a connection.
func (s *subscriptions) unsubAll(c *incomingConn) {
	s.mu.Lock()
	for _, v := range s.subs {
		for i := range v {
			if v[i] == c {
				v[i] = nil
			}
		}
	}

	// remove any associated entries in the wildcard list
	var wildNew []wild
	for i := 0; i < len(s.wildcards); i++ {
		if s.wildcards[i].c != c {
			wildNew = append(wildNew, s.wildcards[i])
		}
	}
	s.wildcards = wildNew

	s.mu.Unlock()
}

// Remove the subscription to topic for a given connection.
func (s *subscriptions) unsub(topic string, c *incomingConn) {
	s.mu.Lock()
	if subs, ok := s.subs[topic]; ok {
		nils := 0

		// Search the list, removing references to our connection.
		// At the same time, count the nils to see if this list is now empty.
		for i := 0; i < len(subs); i++ {
			if subs[i] == c {
				subs[i] = nil
			}
			if subs[i] == nil {
				nils++
			}
		}

		if nils == len(subs) {
			delete(s.subs, topic)
		}
	}
	s.mu.Unlock()
}

// The subscription processing worker.
func (s *subscriptions) run(id int) {
	tag := fmt.Sprintf("worker %d ", id)
	log.Print(tag, "started")
	for post := range s.posts {
		// Remember the original retain setting, but send out immediate
		// copies without retain: "When a server sends a PUBLISH to a client
		// as a result of a subscription that already existed when the
		// original PUBLISH arrived, the Retain flag should not be set,
		// regardless of the Retain flag of the original PUBLISH.
		isRetain := post.m.Header.Retain
		post.m.Header.Retain = false

		// Handle "retain with payload size zero = delete retain".
		// Once the delete is done, return instead of continuing.
		if isRetain && post.m.Payload.Size() == 0 {
			s.mu.Lock()
			delete(s.retain, post.m.TopicName)
			s.mu.Unlock()
			return
		}

		// Find all the connections that should be notified of this message.
		conns := s.subscribers(post.m.TopicName)

		// Queue the outgoing messages
		for _, c := range conns {
			// Do not echo messages back to where they came from.
			if c == post.c {
				continue
			}

			if c != nil {
				c.submit(post.m)
			}
		}

		if isRetain {
			s.mu.Lock()
			// Save a copy of it, and set that copy's Retain to true, so that
			// when we send it out later we notify new subscribers that this
			// is an old message.
			msg := *post.m
			msg.Header.Retain = true
			s.retain[post.m.TopicName] = retain{m: msg}
			s.mu.Unlock()
		}
	}
}

func (s *subscriptions) submit(c *incomingConn, m *proto.Publish) {
	s.posts <- post{c: c, m: m}
}

// A post is a unit of work for the subscription processing workers.
type post struct {
	c *incomingConn
	m *proto.Publish
}

// A Server holds all the state associated with an MQTT server.
type Server struct {
	l             net.Listener
	subs          *subscriptions
	stats         *stats
	Done          chan struct{}
	StatsInterval time.Duration // Defaults to 10 seconds. Must be set using sync/atomic.StoreInt64().
	Dump          bool          // When true, dump the messages in and out.
	rand          *rand.Rand
}

// NewServer creates a new MQTT server, which accepts connections from
// the given listener. When the server is stopped (for instance by
// another goroutine closing the net.Listener), channel Done will become
// readable.
func NewServer(l net.Listener) *Server {
	svr := &Server{
		l:             l,
		stats:         &stats{},
		Done:          make(chan struct{}),
		StatsInterval: time.Second * 10,
		subs:          newSubscriptions(runtime.GOMAXPROCS(0)),
	}

	// start the stats reporting goroutine
	go func() {
		for {
			svr.stats.publish(svr.subs, svr.StatsInterval)
			select {
			case <-svr.Done:
				return
			default:
				// keep going
			}
			time.Sleep(svr.StatsInterval)
		}
	}()

	return svr
}

// Start makes the Server start accepting and handling connections.
func (s *Server) Start() {
	go func() {
		for {
			conn, err := s.l.Accept()
			if err != nil {
				log.Print("Accept: ", err)
				break
			}

			cli := s.newIncomingConn(conn)
			s.stats.clientConnect()
			cli.start()
		}
		close(s.Done)
	}()
}

// An IncomingConn represents a connection into a Server.
type incomingConn struct {
	svr      *Server
	conn     net.Conn
	jobs     chan job
	clientid string
	Done     chan struct{}
}

var clients = make(map[string]*incomingConn)
var clientsMu sync.Mutex

const sendingQueueLength = 10000

// newIncomingConn creates a new incomingConn associated with this
// server. The connection becomes the property of the incomingConn
// and should not be touched again by the caller until the Done
// channel becomes readable.
func (s *Server) newIncomingConn(conn net.Conn) *incomingConn {
	return &incomingConn{
		svr:  s,
		conn: conn,
		jobs: make(chan job, sendingQueueLength),
		Done: make(chan struct{}),
	}
}

type receipt chan struct{}

// Wait for the receipt to indicate that the job is done.
func (r receipt) wait() {
	// TODO: timeout
	<-r
}

type job struct {
	m proto.Message
	r receipt
}

// Start reading and writing on this connection.
func (c *incomingConn) start() {
	go c.reader()
	go c.writer()
}

// Add this connection to the map, or find out that an existing connection
// already exists for the same client-id.
func (c *incomingConn) add() *incomingConn {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	existing, ok := clients[c.clientid]
	if ok {
		// this client id already exists, return it
		return existing
	}

	clients[c.clientid] = c
	return nil
}

// Delete a connection; the connection must be closed by the caller first.
func (c *incomingConn) del() {
	clientsMu.Lock()
	delete(clients, c.clientid)
	clientsMu.Unlock()
	return
}

// Queue a message; no notification of sending is done.
func (c *incomingConn) submit(m proto.Message) {
	j := job{m: m}
	select {
	case c.jobs <- j:
	default:
		log.Print(c, ": failed to submit message")
	}
	return
}

func (c *incomingConn) String() string {
	return fmt.Sprintf("{IncomingConn: %v}", c.clientid)
}

// Queue a message, returns a channel that will be readable
// when the message is sent.
func (c *incomingConn) submitSync(m proto.Message) receipt {
	j := job{m: m, r: make(receipt)}
	c.jobs <- j
	return j.r
}

func (c *incomingConn) reader() {
	// On exit, close the connection and arrange for the writer to exit
	// by closing the output channel.
	defer func() {
		c.conn.Close()
		c.svr.stats.clientDisconnect()
		close(c.jobs)
	}()

	for {
		// TODO: timeout (first message and/or keepalives)
		m, err := proto.DecodeOneMessage(c.conn, nil)
		if err != nil {
			if err == io.EOF {
				return
			}
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				return
			}
			log.Print("reader: ", err)
			return
		}
		c.svr.stats.messageRecv()

		if c.svr.Dump {
			log.Printf("dump  in: %T", m)
		}

		switch m := m.(type) {
		case *proto.Connect:
			rc := proto.RetCodeAccepted

			if m.ProtocolName != "MQIsdp" ||
				m.ProtocolVersion != 3 {
				log.Print("reader: reject connection from ", m.ProtocolName, " version ", m.ProtocolVersion)
				rc = proto.RetCodeUnacceptableProtocolVersion
			}

			// Check client id.
			if len(m.ClientId) < 1 || len(m.ClientId) > 23 {
				rc = proto.RetCodeIdentifierRejected
			}
			c.clientid = m.ClientId

			// Disconnect existing connections.
			if existing := c.add(); existing != nil {
				disconnect := &proto.Disconnect{}
				r := existing.submitSync(disconnect)
				r.wait()
				c.add()
			}

			// TODO: Last will

			connack := &proto.ConnAck{
				ReturnCode: rc,
			}
			c.submit(connack)

			// close connection if it was a bad connect
			if rc != proto.RetCodeAccepted {
				log.Printf("Connection refused for %v: %v", c.conn.RemoteAddr(), ConnectionErrors[rc])
				return
			}

			// Log in mosquitto format.
			clean := 0
			if m.CleanSession {
				clean = 1
			}
			log.Printf("New client connected from %v as %v (c%v, k%v).", c.conn.RemoteAddr(), c.clientid, clean, m.KeepAliveTimer)

		case *proto.Publish:
			// TODO: Proper QoS support
			if m.Header.QosLevel != proto.QosAtMostOnce {
				log.Printf("reader: no support for QoS %v yet", m.Header.QosLevel)
				return
			}
			if m.Header.QosLevel != proto.QosAtMostOnce && m.MessageId == 0 {
				// Invalid message ID. See MQTT-2.3.1-1.
				log.Printf("reader: invalid MessageId in PUBLISH.")
				return
			}
			if isWildcard(m.TopicName) {
				log.Print("reader: ignoring PUBLISH with wildcard topic ", m.TopicName)
			} else {
				c.svr.subs.submit(c, m)
			}
			c.submit(&proto.PubAck{MessageId: m.MessageId})

		case *proto.PingReq:
			c.submit(&proto.PingResp{})

		case *proto.Subscribe:
			if m.Header.QosLevel != proto.QosAtLeastOnce {
				// protocol error, disconnect
				return
			}
			if m.MessageId == 0 {
				// Invalid message ID. See MQTT-2.3.1-1.
				log.Printf("reader: invalid MessageId in SUBSCRIBE.")
				return
			}
			suback := &proto.SubAck{
				MessageId: m.MessageId,
				TopicsQos: make([]proto.QosLevel, len(m.Topics)),
			}
			for i, tq := range m.Topics {
				// TODO: Handle varying QoS correctly
				c.svr.subs.add(tq.Topic, c)
				suback.TopicsQos[i] = proto.QosAtMostOnce
			}
			c.submit(suback)

			// Process retained messages.
			for _, tq := range m.Topics {
				c.svr.subs.sendRetain(tq.Topic, c)
			}

		case *proto.Unsubscribe:
			if m.Header.QosLevel != proto.QosAtMostOnce && m.MessageId == 0 {
				// Invalid message ID. See MQTT-2.3.1-1.
				log.Printf("reader: invalid MessageId in UNSUBSCRIBE.")
				return
			}
			for _, t := range m.Topics {
				c.svr.subs.unsub(t, c)
			}
			ack := &proto.UnsubAck{MessageId: m.MessageId}
			c.submit(ack)

		case *proto.Disconnect:
			return

		default:
			log.Printf("reader: unknown msg type %T", m)
			return
		}
	}
}

func (c *incomingConn) writer() {

	// Close connection on exit in order to cause reader to exit.
	defer func() {
		c.conn.Close()
		c.del()
		c.svr.subs.unsubAll(c)
	}()

	for job := range c.jobs {
		if c.svr.Dump {
			log.Printf("dump out: %T", job.m)
		}

		// TODO: write timeout
		err := job.m.Encode(c.conn)
		if job.r != nil {
			// notifiy the sender that this message is sent
			close(job.r)
		}
		if err != nil {
			// This one is not interesting; it happens when clients
			// disappear before we send their acks.
			oe, isoe := err.(*net.OpError)
			if isoe && oe.Err.Error() == "use of closed network connection" {
				return
			}
			// In Go < 1.5, the error is not an OpError.
			if err.Error() == "use of closed network connection" {
				return
			}

			log.Print("writer: ", err)
			return
		}
		c.svr.stats.messageSend()

		if _, ok := job.m.(*proto.Disconnect); ok {
			log.Print("writer: sent disconnect message")
			return
		}
	}
}

// header is used to initialize a proto.Header when the zero value
// is not correct. The zero value of proto.Header is
// the equivalent of header(dupFalse, proto.QosAtMostOnce, retainFalse)
// and is correct for most messages.
func header(d dupFlag, q proto.QosLevel, r retainFlag) proto.Header {
	return proto.Header{
		DupFlag: bool(d), QosLevel: q, Retain: bool(r),
	}
}

type retainFlag bool
type dupFlag bool

const (
	retainFalse retainFlag = false
	retainTrue             = true
	dupFalse    dupFlag    = false
	dupTrue                = true
)

func isWildcard(topic string) bool {
	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return true
	}
	return false
}

func (w wild) valid() bool {
	for i, part := range w.wild {
		// catch things like finance#
		if isWildcard(part) && len(part) != 1 {
			return false
		}
		// # can only occur as the last part
		if part == "#" && i != len(w.wild)-1 {
			return false
		}
	}
	return true
}

const clientQueueLength = 100

// A ClientConn holds all the state associated with a connection
// to an MQTT server. It should be allocated via NewClientConn.
// Concurrent access to a ClientConn is NOT safe.
type ClientConn struct {
	ClientId string              // May be set before the call to Connect.
	Dump     bool                // When true, dump the messages in and out.
	Incoming chan *proto.Publish // Incoming messages arrive on this channel.
	id       uint16              // next MessageId
	out      chan job
	conn     net.Conn
	done     chan struct{} // This channel will be readable once a Disconnect has been successfully sent and the connection is closed.
	connack  chan *proto.ConnAck
	suback   chan *proto.SubAck
}

// NewClientConn allocates a new ClientConn.
func NewClientConn(c net.Conn) *ClientConn {
	cc := &ClientConn{
		conn:     c,
		id:       1,
		out:      make(chan job, clientQueueLength),
		Incoming: make(chan *proto.Publish, clientQueueLength),
		done:     make(chan struct{}),
		connack:  make(chan *proto.ConnAck),
		suback:   make(chan *proto.SubAck),
	}
	go cc.reader()
	go cc.writer()
	return cc
}

func (c *ClientConn) reader() {
	defer func() {
		// Cause the writer to exit.
		close(c.out)
		// Cause any goroutines waiting on messages to arrive to exit.
		close(c.Incoming)
		c.conn.Close()
	}()

	for {
		// TODO: timeout (first message and/or keepalives)
		m, err := proto.DecodeOneMessage(c.conn, nil)
		if err != nil {
			if err == io.EOF {
				return
			}
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				return
			}
			log.Print("cli reader: ", err)
			return
		}

		if c.Dump {
			log.Printf("dump  in: %T", m)
		}

		switch m := m.(type) {
		case *proto.Publish:
			c.Incoming <- m
		case *proto.PubAck:
			// ignore these
			continue
		case *proto.ConnAck:
			c.connack <- m
		case *proto.SubAck:
			c.suback <- m
		case *proto.Disconnect:
			return
		default:
			log.Printf("cli reader: got msg type %T", m)
		}
	}
}

func (c *ClientConn) writer() {
	// Close connection on exit in order to cause reader to exit.
	defer func() {
		// Signal to Disconnect() that the message is on its way, or
		// that the connection is closing one way or the other...
		close(c.done)
	}()

	for job := range c.out {
		if c.Dump {
			log.Printf("dump out: %T", job.m)
		}

		// TODO: write timeout
		err := job.m.Encode(c.conn)
		if job.r != nil {
			close(job.r)
		}

		if err != nil {
			log.Print("cli writer: ", err)
			return
		}

		if _, ok := job.m.(*proto.Disconnect); ok {
			return
		}
	}
}

// Connect sends the CONNECT message to the server. If the ClientId is not already
// set, use a default (a 63-bit decimal random number). The "clean session"
// bit is always set.
func (c *ClientConn) Connect(user, pass string) error {
	// TODO: Keepalive timer
	if c.ClientId == "" {
		c.ClientId = fmt.Sprint(cliRand.Int63())
	}
	req := &proto.Connect{
		ProtocolName:    "MQIsdp",
		ProtocolVersion: 3,
		ClientId:        c.ClientId,
		CleanSession:    true,
	}
	if user != "" {
		req.UsernameFlag = true
		req.PasswordFlag = true
		req.Username = user
		req.Password = pass
	}

	c.sync(req)
	ack := <-c.connack
	return ConnectionErrors[ack.ReturnCode]
}

// ConnectionErrors is an array of errors corresponding to the
// Connect return codes specified in the specification.
var ConnectionErrors = [6]error{
	nil, // Connection Accepted (not an error)
	errors.New("Connection Refused: unacceptable protocol version"),
	errors.New("Connection Refused: identifier rejected"),
	errors.New("Connection Refused: server unavailable"),
	errors.New("Connection Refused: bad user name or password"),
	errors.New("Connection Refused: not authorized"),
}

// Disconnect sends a DISCONNECT message to the server. This function
// blocks until the disconnect message is actually sent, and the connection
// is closed.
func (c *ClientConn) Disconnect() {
	c.sync(&proto.Disconnect{})
	<-c.done
}

func (c *ClientConn) nextid() uint16 {
	id := c.id
	c.id++
	return id
}

// Subscribe subscribes this connection to a list of topics. Messages
// will be delivered on the Incoming channel.
func (c *ClientConn) Subscribe(tqs []proto.TopicQos) *proto.SubAck {
	c.sync(&proto.Subscribe{
		Header:    header(dupFalse, proto.QosAtLeastOnce, retainFalse),
		MessageId: c.nextid(),
		Topics:    tqs,
	})
	ack := <-c.suback
	return ack
}

// Publish publishes the given message to the MQTT server.
// The QosLevel of the message must be QosAtLeastOnce for now.
func (c *ClientConn) Publish(m *proto.Publish) {
	if m.QosLevel != proto.QosAtMostOnce {
		panic("unsupported QoS level")
	}
	m.MessageId = c.nextid()
	c.out <- job{m: m}
}

// sync sends a message and blocks until it was actually sent.
func (c *ClientConn) sync(m proto.Message) {
	j := job{m: m, r: make(receipt)}
	c.out <- j
	<-j.r
	return
}
