package main

import (
	mqttServer "github.com/ctlove0523/mqtt-brokers/broker"
	"time"
)

func main() {
	broker := mqttServer.NewServer("localhost", 8883)

	go func() {
		broker.Start()
	}()

	for ; ; {
		broker.PublishMsg("test", 1, []byte("hello client"))
		time.Sleep(5 * time.Second)
	}
}
