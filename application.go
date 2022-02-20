package main

import (
	"github.com/ctlove0523/mqtt-brokers/broker"
	"time"
)

func main() {
	server := broker.NewServer("localhost", 8883)

	go func() {
		server.Start()
	}()

	for ; ; {
		time.Sleep(10 * time.Second)
		server.PublishMsg("test", 1, []byte("hello client"))
	}
}
