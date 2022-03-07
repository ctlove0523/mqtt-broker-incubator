package main

import (
	"github.com/ctlove0523/mqtt-brokers/broker"
	"time"
)

func main() {
	server, _ := broker.NewServer("localhost", 8883)

	go func() {
		server.Start()
	}()

	time.Sleep(10 * time.Second)
	server.PublishMsg("test", 1, []byte("hello client"))
	ch := make(chan struct{})
	<-ch
}
