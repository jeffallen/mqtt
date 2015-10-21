mqtt
====

MQTT Clients, Servers and Load Testers in Go

For docs, see: http://godoc.org/github.com/jeffallen/mqtt

For a little discussion of this code see: http://blog.nella.org/mqtt-code-golf

Limitations
-----------

At this time, the following limitations apply:
 * QoS level 0 only; messages are only stored in RAM
 * Retain works, but at QoS level 0. Retained messages are lost on server restart.
 * Last will messages are not implemented.
 * Keepalive and timeouts are not implemented.

Servers
-------

The example MQTT servers are in directories <tt>mqttsrv</tt> and <tt>smqttsrv</tt> (secured with TLS).

Benchmarking Tools
------------------

To use the benchmarking tools, cd into <tt>pingtest</tt>, <tt>loadtest</tt>, or <tt>many</tt> and type "go build". The tools have reasonable defaults, but you'll also want to use the -help flag to find out what can be tuned.

All benchmarks suck, and these three suck in different ways.

pingtest simulates a number of pairs of clients who are bouncing messages between them as fast as possible. It aims to measure latency of messages through the system when under load.

loadtest simulates a number of pairs of clients where one is sending as fast as possible to the other. Realistically, this ends up testing the ability of the system to decode and queue messages, because any slight inbalance in scheduling of readers causes a pile up of messages from the writer slamming them down the throat of the server.

many simulates a large number of clients who send a low transaction rate. The goal is to eventually use this to achieve 1 million (and more?) concurrent, active MQTT sessions in one server. So far, <tt>mqttsrv</tt> has survived a load of 40k concurrent connections from <tt>many</tt>.

Travis Build
------------

![build status](https://travis-ci.org/jeffallen/mqtt.svg?branch=master)
