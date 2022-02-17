package broker

import (
	"bufio"
	"fmt"
	"github.com/ctlove0523/mqtt-brokers/broker/packets"
	"io"
	"net"
	"time"
)

// 代表一个client和server的连接
type Connection struct {
	socket   net.Conn
	server   *Server
	clientId []byte
	connect  *packets.ConnectPacket
}

func (c *Connection) Close() {
	err := c.socket.Close()
	if err != nil {
		fmt.Printf("close connection failed %v", err)
	} else {
		fmt.Println("close connection success")
	}
}

// Process 处理连接上的请求
func (c *Connection) Process() error {
	defer c.Close()
	reader := bufio.NewReaderSize(c.socket, 65536)
	maxSize := int64(10240)
	for {
		// read/write 限制以处理悬空连接
		err := c.socket.SetDeadline(time.Now().Add(time.Second * 120))
		if err != nil {
			fmt.Printf("conn set deadline failed %v", err)
		}

		// 解码 MQTT packet
		msg, err := DecodePacket(reader, maxSize)
		if err != nil {
			return err
		}

		// 处理接收的MQTT消息
		if err := c.processMqttMessage(msg); err != nil {
			return err
		}
	}
}

// processMqttMessage handles an MQTT receive.
func (c *Connection) processMqttMessage(msg packets.MqttPacket) error {
	switch msg.Type() {
	// 连接请求
	case packets.TypeOfConnect:
		connMsg := msg.(*packets.ConnectPacket)
		clientId := string(connMsg.ClientID)
		_, ok := c.server.clients[clientId]
		if ok {
			fmt.Printf("connection with id %s already exist\n", clientId)
			c.server.Disconnect(clientId)
		}

		var result uint8
		if !c.onConnect(connMsg) {
			result = 0x05 // Unauthorized
		}

		// Write the ack
		ack := packets.ConnAckPacket{ReturnCode: result}
		if _, err := ack.EncodeTo(c.socket); err != nil {
			return err
		}

	// We got an attempt to subscribe to a channel.
	case packets.TypeOfSubscribe:
		packet := msg.(*packets.SubscribePacket)
		ack := packets.SubAckPacket{
			Header: packets.FixedHeader{
				MessageType: 9,
			},
			MessageID: packet.MessageID,
			Qos:       make([]uint8, 0, len(packet.Subscriptions)),
		}

		// Subscribe for each subscription
		for _, sub := range packet.Subscriptions {
			fmt.Println(sub.Topic)
			if !c.server.onSubscribe(string(sub.Topic), string(c.clientId)) {
				ack.Qos = append(ack.Qos, 0x80) // 0x80 indicate subscription failure
				continue
			}

			// Append the QoS
			ack.Qos = append(ack.Qos, sub.Qos)
		}

		// Acknowledge the subscription
		if _, err := ack.EncodeTo(c.socket); err != nil {
			return err
		}

	// We got an attempt to unsubscribe from a channel.
	case packets.TypeOfUnsubscribe:
		packet := msg.(*packets.Unsubscribe)
		ack := packets.UnsubAckPacket{MessageID: packet.MessageID}

		// Unsubscribe from each subscription
		for _, sub := range packet.Topics {
			fmt.Println(sub.Topic)
			c.server.onUnsubscribe(string(sub.Topic), string(c.clientId))

		}

		// Acknowledge the unsubscription
		if _, err := ack.EncodeTo(c.socket); err != nil {
			return err
		}

	// We got an MQTT ping response, respond appropriately.
	case packets.TypeOfPingReq:
		ack := packets.PingResp{}
		if _, err := ack.EncodeTo(c.socket); err != nil {
			return err
		}

	case packets.TypeOfDisconnect:
		return io.EOF

	case packets.TypeOfPublish:
		packet := msg.(*packets.PublishPacket)
		fmt.Println("get message " + string(packet.Payload))
		msg := Message{
			Id:      packet.MessageID,
			Topic:   packet.TopicName,
			Payload: packet.Payload,
		}
		c.server.OnMessage(msg)

		// Acknowledge the publication
		if packet.Header.Qos > 0 {
			ack := packets.PubAckPacket{MessageID: packet.MessageID}
			if _, err := ack.EncodeTo(c.socket); err != nil {
				return err
			}
		}

	case packets.TypeOfPubAck:
		packet := msg.(*packets.PubAckPacket)
		fmt.Println("get pub ack")
		fmt.Println(packet.MessageID)
		c.server.onPubAck(string(c.clientId), packet.MessageID)
	}

	return nil
}

// onConnect handles the connection authorization
func (c *Connection) onConnect(packet *packets.ConnectPacket) bool {
	fmt.Printf("version is %d\n", packet.Version)
	// 鉴权
	authResult := true
	if c.server.AuthFunction != nil {
		authResult = c.server.AuthFunction(packet.ClientID, packet.Username, packet.Password)
	}

	if authResult {
		c.clientId = packet.ClientID
		c.server.onConnect(c)
	}

	c.connect = &packets.ConnectPacket{
		ProtoName:        packet.ProtoName,
		Version:          packet.Version,
		UsernameFlag:     packet.UsernameFlag,
		PasswordFlag:     packet.PasswordFlag,
		WillRetainFlag:   packet.WillRetainFlag,
		WillQOS:          packet.WillQOS,
		WillFlag:         packet.WillFlag,
		CleanSessionFlag: packet.CleanSessionFlag,
		KeepAlive:        packet.KeepAlive,
		ClientID:         packet.ClientID,
		WillTopic:        packet.WillTopic,
		WillMessage:      packet.WillMessage,
		Username:         packet.Username,
		Password:         packet.Password,
	}

	return authResult
}
