package broker

import (
	"fmt"
	"github.com/ctlove0523/mqtt-brokers/broker/packets"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type AuthFunction func(clientId, userName, password []byte) bool

type MessageHandler func(msg Message) bool

type Server struct {
	Address       string
	Port          int
	AuthFunction  AuthFunction
	msgHandlers   []MessageHandler
	clients       map[string]*Connection
	subscriptions map[string][]string      // 订阅topic的客户端
	clientSubs    map[string]*Subscription // 客户端订阅的topic
	msgIds        map[string]uint16
	subStore      SubscriptionStore

	// QOS1
	waitAckMessageIds map[string]map[uint16]bool // 存储每个client等待确认的消息
	waitAckMessage    map[int64][]Qos1Message    // 等待确认的消息内容
	timerManager      *cron.Cron

	startTime   int64 // server启动时间
	startSwitch chan struct{}

	Log *zap.Logger
}

func NewServer(address string, port int) (*Server, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Println("create zap logger failed")
		return nil, err
	}
	defer logger.Sync()

	s := &Server{
		Address: address,
		Port:    port,
		AuthFunction: func(clientId, userName, password []byte) bool {
			return true
		},
		msgHandlers:       []MessageHandler{},
		clients:           make(map[string]*Connection),
		subscriptions:     make(map[string][]string),
		clientSubs:        make(map[string]*Subscription),
		msgIds:            make(map[string]uint16),
		subStore:          NewPebbleSubscriptionStore("sub"),
		timerManager:      cron.New(cron.WithSeconds()),
		waitAckMessageIds: map[string]map[uint16]bool{},
		waitAckMessage:    map[int64][]Qos1Message{},
		startSwitch:       make(chan struct{}),
		Log:               logger,
	}

	return s, nil
}

func (s *Server) Start() {
	s.Log.Info("begin to start mqtt server")
	s.startTime = time.Now().Unix()
	listener, err := net.Listen("tcp", strings.Join([]string{s.Address, strconv.Itoa(s.Port)}, ":"))
	if err != nil {
		s.Log.Error("server start failed:", zap.Int("port", s.Port), zap.String("address", s.Address))
		return
	}

	// 初始化订阅数据
	subs := s.subStore.listAllSubscriptions()
	for _, v := range subs {
		clientId := string((*v).ClientId)
		s.clientSubs[clientId] = v

		var topics []string
		for _, t := range v.Topics {
			topics = append(topics, t.Topic)
		}
		for _, tt := range topics {
			clientIds, ok := s.subscriptions[tt]
			if !ok {
				clientIds = []string{}
			}
			clientIds = append(clientIds, clientId)
			s.subscriptions[tt] = clientIds
		}

	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Printf("create new conn failed %v", err)
				continue
			}

			mqttConn := s.newConnection(conn)
			go func() {
				err = mqttConn.Process()
				if err != nil {
					return
				}
			}()
		}
	}()

	// 启动qos1定时任务
	_, err = s.timerManager.AddJob("0/20 * * * * ? ", s)
	if err != nil {
		s.Log.Error("add qosq job failed")
	}

	s.timerManager.Start()

	s.Log.Info("start mqtt server success")
	<-s.startSwitch
}

func (s *Server) Close() {
	for _, v := range s.clients {
		v.Close()
	}

	s.timerManager.Stop()
	s.startSwitch <- struct{}{}
}

func (s *Server) Disconnect(clientId string) {
	client, ok := s.clients[clientId]
	if ok {
		client.Close()
		delete(s.clients, clientId)
	}
}

func (s *Server) SetAuthFunction(function AuthFunction) {
	s.AuthFunction = function
}

func (s *Server) newConnection(conn net.Conn) *Connection {
	mqttConn := &Connection{
		socketState: connOpen,
		socket:      conn,
		server:      s,
	}

	return mqttConn
}

func (s *Server) onSubscribe(topic, clientId string, qos uint8) bool {
	fmt.Println("begin to process client sub")
	log.Printf("begin to process client sub,topic = %s,clinet id = %s,qos = %d", topic, clientId, qos)

	// 订阅topic的客户端
	clientIds, ok := s.subscriptions[topic]
	if !ok {
		clientIds = []string{}
	} else {
		clientIds = deleteElement(clientIds, clientId)
	}
	clientIds = append(clientIds, clientId)
	s.subscriptions[topic] = clientIds

	// 客户端订阅的topic
	topics, ok := s.clientSubs[clientId]
	if !ok {
		topics = &Subscription{
			ClientId: []byte(clientId),
			Topics:   []TopicQos{},
		}
	}
	topics.Topics = append(topics.Topics, TopicQos{
		Topic: topic,
		Qos:   qos,
	})
	s.clientSubs[clientId] = topics

	s.subStore.addNewSubscription(*topics)
	return true
}

func (s *Server) onUnsubscribe(topic, clientId string, qos uint8) bool {
	_, ok := s.clients[clientId]
	if !ok {
		fmt.Println("client id not exit")
		return false
	}

	clientIds, ok := s.subscriptions[topic]
	if !ok {
		fmt.Println("topic not exist")
		return false
	}

	clientIds = deleteElement(clientIds, clientId)
	s.subscriptions[topic] = clientIds

	topics, ok := s.clientSubs[clientId]
	if !ok {
		fmt.Println("client not sub  topic")
		return false
	}

	topics.Topics = deleteTopicQos(topics.Topics, TopicQos{Topic: topic, Qos: qos})
	s.clientSubs[clientId] = topics

	s.subStore.removeSubscription(topic, clientId)
	return true
}

func (s *Server) onMessage(msg Message) bool {
	if len(s.msgHandlers) != 0 {
		for _, handler := range s.msgHandlers {
			handler(msg)
		}
	}

	return true
}

func (s *Server) onConnect(conn *Connection) {
	s.clients[string(conn.clientId)] = conn
}

func (s *Server) onPubAck(clientId string, messageId uint16) {
	fmt.Printf("get message ack from client %s fir nessage %d\n", clientId, messageId)
	messageIds, ok := s.waitAckMessageIds[clientId]
	if !ok {
		return
	}
	delete(messageIds, messageId)
}

func (s *Server) PublishMsg(topic string, qos byte, payload []byte) {
	clientIds := s.subscriptions[topic]
	if len(clientIds) == 0 {
		return
	} else {
		fmt.Println(clientIds)
	}

	pubPacket := &packets.PublishPacket{}
	pubPacket.Header.MessageType = 3
	pubPacket.Header.Dup = false
	pubPacket.Header.Qos = qos
	pubPacket.TopicName = []byte(topic)
	pubPacket.Payload = payload

	for _, clientId := range clientIds {
		conn, ok := s.clients[clientId]
		if !ok {
			continue
		}
		if qos == 1 {
			pubPacket.MessageID = s.messageId(clientId)

			// 存储QOS = 1的消息未重新发送做准备
			waitClientAckMessageIds, ok := s.waitAckMessageIds[clientId]
			if ok {
				waitClientAckMessageIds[pubPacket.MessageID] = true
			} else {
				waitClientAckMessageIds = map[uint16]bool{}
				waitClientAckMessageIds[pubPacket.MessageID] = true
				s.waitAckMessageIds[clientId] = waitClientAckMessageIds
			}

			qos1Msg := Qos1Message{
				topic:     topic,
				payload:   payload,
				MessageID: pubPacket.MessageID,
			}

			slot := (time.Now().Unix() - s.startTime) / 20
			log.Printf("slot is %d\n", slot)
			ackMessages, ok := s.waitAckMessage[slot]
			if ok {
				ackMessages = append(ackMessages, qos1Msg)
			} else {
				ackMessages = make([]Qos1Message, 0)
				ackMessages = append(ackMessages, qos1Msg)
				s.waitAckMessage[slot] = ackMessages
			}

		} else {
			pubPacket.MessageID = 0
		}

		_, err := pubPacket.EncodeTo(conn.socket)
		if err != nil {
			fmt.Println(err)
		}
	}
}

type Qos1Message struct {
	topic     string
	payload   []byte
	MessageID uint16
}

func (s *Server) messageId(clientId string) uint16 {
	s.msgIds[clientId] += 1
	return s.msgIds[clientId]
}

func (s *Server) Run() {
	slot := (time.Now().Unix()-s.startTime)/20 - 1
	log.Printf("run slot is %d\n", slot)
	ackMessages, ok := s.waitAckMessage[slot]
	if !ok {
		fmt.Println("no need republish messages")
		return
	}
	for _, msg := range ackMessages {
		topic := msg.topic

		clientIds, ok := s.subscriptions[topic]
		if !ok {
			fmt.Printf("no client sub %s\n", topic)
			continue
		}

		pubPacket := &packets.PublishPacket{}
		pubPacket.Header.MessageType = 3
		pubPacket.Header.Dup = true
		pubPacket.Header.Qos = 1
		pubPacket.TopicName = []byte(topic)
		pubPacket.Payload = msg.payload
		pubPacket.MessageID = msg.MessageID
		for _, clientId := range clientIds {
			conn, ok := s.clients[clientId]
			if !ok {
				continue
			}
			_, err := pubPacket.EncodeTo(conn.socket)
			if err != nil {
				fmt.Println("republish ")
			}
		}
	}

	delete(s.waitAckMessage, slot)
}

func deleteElement(container []string, element string) []string {
	index := -1
	for i := 0; i < len(container); i++ {
		if container[i] == element {
			index = i
			break
		}
	}
	if index == -1 {
		return container
	}

	var newContainer []string
	newContainer = append(newContainer, container[:index]...)
	newContainer = append(newContainer, container[index+1:]...)
	return newContainer
}

func deleteTopicQos(topicQos []TopicQos, element TopicQos) []TopicQos {
	index := -1
	for i := 0; i < len(topicQos); i++ {
		if topicQos[i].Qos == element.Qos && topicQos[i].Topic == element.Topic {
			index = i
			break
		}
	}
	if index == -1 {
		return topicQos
	}

	var newContainer []TopicQos
	newContainer = append(newContainer, topicQos[:index]...)
	newContainer = append(newContainer, topicQos[index+1:]...)
	return newContainer
}

type Message struct {
	Id      uint16
	Topic   []byte
	Payload []byte
}

type Subscription struct {
	ClientId []byte
	Topics   []TopicQos
}

type TopicQos struct {
	Qos   uint8
	Topic string
}
