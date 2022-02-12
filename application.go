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

	time.Sleep(20*time.Second)
	mes := &packets.PublishPacket{
		TopicName:   []byte("test"),
		Payload: []byte("hello"),
	}
	broker.Publish(mes)

	time.Sleep(10*time.Second)
	broker.Publish(mes)


	time.Sleep(300 * time.Second)
}
