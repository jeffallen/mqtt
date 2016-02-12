Vbridge
=======

Vbridge is a bridge for message brokers which uses [Vanadium] (https://vanadium.github.io)
for RPC that can securely and reliably cross network borders.

Vbridge uses Vanadium's input and output streams to move MQTT messages
in both directions between the linked brokers. It is not currently
possible to limit replication to one direction or the other.

Vbridge operates in one of three modes: server, client, or pipe (dual client).

Server
------

As a server, vbridge implements the [Bridge service]
(https://github.com/jeffallen/mqtt/tree/master/vbridge/ifc) and
bridges messages to/from the caller into a the MQTT server it is
configured to attach to.

To run it in server mode, use "./vbridge -service-name XXX". XXX will
be registered the Vanadium namespace, so that callers can find the
service.

Client
------

vbridge can connect to an MQTT broker as a client and then bridge
messages to another instance of Vbridge.

To run it in client mode, use the "-to" argument to tell it which
Vanadium service name it should be connecting to.

Pipe
----

vbridge can act as a pipe, connecting two servers which both
provide the Bridge service.

To run it in pipe mode, use the "-to" and "-from" arguments to tell it
which two Vanadium service names it should be linking. There's no
practical difference between to and from, they are just two flags to
make it clearer what's happening.

Echoing
-------

Vbridge uses one MQTT connection for both sending and receiving
messages.  The MQTT specification, though it does not clearly indicate
it, implies that a message that is published on a given connection
should be echoed back to the same connection, if that connection is
subscribed to a matching topic.

If that were the case, it would cause vbridge to create an endless
message loop.

Therefore, the MQTT server in this Git repository has been patched to
not send these echo messages, because that makes vbridge work, and
because it just makes more sense that way.

If you need vbridge to work with a message broker that has this echo
"behavior" (or "problem", depending on how you look at it) then you'll
need to patch the broker to not do that.

Example
-------

The following set of commands shows the system working:

    mqttsrv/mqttsrv -addr :1883 &
    mqttsrv/mqttsrv -addr :1884 &
    ticktock/ticktock -host :1883 -who bonnie &
    ticktock/ticktock -host :1884 -who clyde &
    vbridge/vbridge -service-name tmp/$USER/vbridge -host :1883 -topics tick &
    vbridge/vbridge -to tmp/$USER/vbridge -host :1884 -topics tick

(You might want to run the ticktocks in two windows, to see
separately what each of them is saying.)
	
Ticktock is a program that subscribes to topic "tick"
while publishing messages on that topic, once a second.
(see ticktock -help for options).

The first vbridge is running as a server. It registers
itself as service tmp/$USER/vbridge in your default
mount table (usually in dev.v.io).

The second vbridge is running as a client. It looks up
the first one in the mount table, and calls it.

Once the sixth command is run (the client vbridge) the ticktocks
will start showing messages from the other ticktock. But
because each ticktock is connected only to it's "own"
MQTT server, this demonstrates that the bridge is functioning.

If you stop and start the client vbridge, the messages stop
and start in the ticktocks.

Issues to resolve
-----------------

Timeouts are not yet handled as expected. There needs to be a
timeout on the call to Send(), since it is designed to block.

It is unclear if the ChannelTimeout is supposed to break the
underlying connection and cause Send() return an error. In any
case, it doesn't work; suspending one vbridge does not make
the other one exit as expected.
