package broker

import (
	"fmt"
	"github.com/ctlove0523/mqtt-brokers/broker/packets"
	"net"
	"strconv"
	"strings"
)

type AuthFunction func(clientId, userName, password []byte) bool

type MessageHandler func(msg Message) bool

type Server struct {
	Address       string
	Port          int
	AuthFunction  AuthFunction
	msgHandlers   []MessageHandler
	clients       map[string]*Connection
	subscriptions map[string][]string // 订阅topic的客户端
	clientSubs    map[string][]string // 客户端订阅的topic
	msgIds        map[string]uint16
}

func NewServer(address string, port int) *Server {
	s := &Server{
		Address: address,
		Port:    port,
		AuthFunction: func(clientId, userName, password []byte) bool {
			return true
		},
		msgHandlers:   []MessageHandler{},
		clients:       make(map[string]*Connection),
		subscriptions: make(map[string][]string),
		clientSubs:    make(map[string][]string),
		msgIds:        make(map[string]uint16),
	}

	return s
}

func (s *Server) newConnection(conn net.Conn) *Connection {
	mqttConn := &Connection{
		socket: conn,
		server: s,
	}

	return mqttConn
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", strings.Join([]string{s.Address, strconv.Itoa(s.Port)}, ":"))
	if err != nil {
		fmt.Printf("server start failed on %s bind %d Port", s.Address, s.Port)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("create new conn failed %v", err)
			continue
		}

		mqttConn := s.newConnection(conn)
		go func() {
			err := mqttConn.Process()
			if err != nil {
				fmt.Printf("process conn failed %v\n", err)
			}
		}()
	}
}

func (s *Server) Close() {
	for _, v := range s.clients {
		v.Close()
	}
}

func (s *Server) Disconnect(clientId string) {
	client, ok := s.clients[clientId]
	if ok {
		client.Close()
		topics := s.clientSubs[clientId]
		delete(s.clients, clientId)
		delete(s.clientSubs, clientId)
		for _, v := range topics {
			clientIds := s.subscriptions[v]
			clientIds = deleteElement(clientIds, clientId)
			s.subscriptions[v] = clientIds
		}
	}
}

func (s *Server) SetAuthFunction(function AuthFunction) {
	s.AuthFunction = function
}

func (s *Server) onSubscribe(topic, clientId string) bool {
	fmt.Println("begin to process client sub")
	clientIds, ok := s.subscriptions[topic]
	if !ok {
		clientIds = []string{}
	}
	clientIds = append(clientIds, clientId)
	s.subscriptions[topic] = clientIds

	topics, ok := s.clientSubs[clientId]
	if !ok {
		topics = []string{}
	}
	topics = append(topics, topic)
	s.clientSubs[clientId] = topics

	return true
}

func (s *Server) onUnsubscribe(topic, clientId string) bool {
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

	topics = deleteElement(topics, topic)
	s.clientSubs[clientId] = topics

	return true
}

func (s *Server) OnMessage(msg Message) bool {
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
}

func (s *Server) PublishMsg(topic string, qos byte, payload []byte) {
	clientIds := s.subscriptions[topic]
	if len(clientIds) == 0 {
		return
	}

	pubPacket := &packets.PublishPacket{}
	pubPacket.Header.MessageType = 3
	pubPacket.Header.Dup = false
	pubPacket.Header.Qos = qos
	pubPacket.TopicName = []byte(topic)
	pubPacket.Payload = payload

	for _, clientId := range clientIds {
		conn := s.clients[clientId]
		if qos > 0 {
			pubPacket.MessageID = s.messageId(clientId)
		} else {
			pubPacket.MessageID = 0
		}

		_, err := pubPacket.EncodeTo(conn.socket)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (s *Server) messageId(clientId string) uint16 {
	s.msgIds[clientId] += 1
	return s.msgIds[clientId]
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

type Message struct {
	Id      uint16
	Topic   []byte
	Payload []byte
}
