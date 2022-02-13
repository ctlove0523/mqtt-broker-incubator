package main

import (
	mqttServer "github.com/ctlove0523/mqtt-brokers/broker"
	"github.com/ctlove0523/mqtt-brokers/broker/packets"
	"time"
)

func main() {
	broker := mqttServer.NewServer("localhost", 8883)

	go func() {
		broker.Start()
	}()

	mes := &packets.PublishPacket{
		TopicName: []byte("test"),
		Payload:   []byte("hello"),
	}

	for ; ; {
		broker.Publish(mes)
		time.Sleep(5 * time.Second)
	}
}
