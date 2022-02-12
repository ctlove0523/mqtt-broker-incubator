package broker

import (
	"fmt"
	"github.com/ctlove0523/mqtt-brokers/broker/packets"
	"io"
)

// Reader is the requred reader for an efficient decoding.
type Reader interface {
	io.Reader
	ReadByte() (byte, error)
}

// DecodePacket decodes the packet from the provided reader.
func DecodePacket(rdr Reader, maxMessageSize int64) (packets.MqttPacket, error) {
	hdr, sizeOf, messageType, err := decodeHeader(rdr)
	if err != nil {
		return nil, err
	}

	// Check for empty packets
	switch messageType {
	case packets.TypeOfPingReq:
		return &packets.PingReq{}, nil
	case packets.TypeOfPingResp:
		return &packets.PingResp{}, nil
	case packets.TypeOfDisconnect:
		return &packets.DisconnectPacket{}, nil
	}

	//check to make sure packet isn't above size limit
	if int64(sizeOf) > maxMessageSize {
		return nil, packets.ErrMessageTooLarge
	}

	// Now we can decode the buffer. The problem here is that we have to create
	// a new buffer for the body as we're going to simply create slices around it.
	// There's probably a way to use a "read buffer" provided to reduce allocations.
	buffer := make([]byte, sizeOf)
	_, err = io.ReadFull(rdr, buffer)
	if err != nil {
		return nil, err
	}

	// Decode the body
	var msg packets.MqttPacket
	switch messageType {
	case packets.TypeOfConnect:
		msg, err = decodeConnect(buffer)
	case packets.TypeOfConnAck:
		msg = decodeConnack(buffer, hdr)
	case packets.TypeOfPublish:
		msg, err = decodePublish(buffer, hdr)
	case packets.TypeOfPubAck:
		msg = decodePuback(buffer)
	case packets.TypeOfPubRec:
		msg = decodePubrec(buffer)
	case packets.TypeOfPubRel:
		msg = decodePubrel(buffer, hdr)
	case packets.TypeOfPubComp:
		msg = decodePubcomp(buffer)
	case packets.TypeOfSubscribe:
		msg, err = decodeSubscribe(buffer, hdr)
	case packets.TypeOfSubAck:
		msg = decodeSuback(buffer)
	case packets.TypeOfUnsubscribe:
		msg, err = decodeUnsubscribe(buffer, hdr)
	case packets.TypeOfUnsubAck:
		msg = decodeUnsuback(buffer)
	default:
		return nil, fmt.Errorf("Invalid zero-length packet with type %d", messageType)
	}

	return msg, err
}

// decodeHeader decodes the header
func decodeHeader(rdr Reader) (hdr packets.FixedHeader, length uint32, messageType uint8, err error) {
	firstByte, err := rdr.ReadByte()
	if err != nil {
		return packets.FixedHeader{}, 0, 0, err
	}

	messageType = (firstByte & 0xf0) >> 4

	// Set the header depending on the message type
	switch messageType {
	case packets.TypeOfPublish, packets.TypeOfSubscribe, packets.TypeOfUnsubscribe, packets.TypeOfPubRel:
		DUP := firstByte&0x08 > 0
		QOS := firstByte & 0x06 >> 1
		retain := firstByte&0x01 > 0

		hdr = packets.FixedHeader{
			Dup:    DUP,
			Qos:    QOS,
			Retain: retain,
		}
	}

	multiplier := uint32(1)
	digit := byte(0x80)

	// Read the length
	for (digit & 0x80) != 0 {
		b, err := rdr.ReadByte()
		if err != nil {
			return packets.FixedHeader{}, 0, 0, err
		}

		digit = b
		length += uint32(digit&0x7f) * multiplier
		multiplier *= 128
	}

	return hdr, uint32(length), messageType, nil
}

func decodeConnect(data []byte) (packets.MqttPacket, error) {
	//TODO: Decide how to recover rom invalid packets (offsets don't equal actual reading?)
	bookmark := uint32(0)

	protoname, err := readString(data, &bookmark)
	if err != nil {
		return nil, err
	}
	ver := uint8(data[bookmark])
	bookmark++
	flags := data[bookmark]
	bookmark++
	keepalive := readUint16(data, &bookmark)
	cliID, err := readString(data, &bookmark)
	if err != nil {
		return nil, err
	}
	connect := &packets.ConnectPacket{
		ProtoName:        protoname,
		Version:          ver,
		KeepAlive:        keepalive,
		ClientID:         cliID,
		UsernameFlag:     flags&(1<<7) > 0,
		PasswordFlag:     flags&(1<<6) > 0,
		WillRetainFlag:   flags&(1<<5) > 0,
		WillQOS:          (flags & (1 << 4)) + (flags & (1 << 3)),
		WillFlag:         flags&(1<<2) > 0,
		CleanSessionFlag: flags&(1<<1) > 0,
	}

	if connect.WillFlag {
		if connect.WillTopic, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
		if connect.WillMessage, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
	}

	if connect.UsernameFlag {
		if connect.Username, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
	}

	if connect.PasswordFlag {
		if connect.Password, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
	}
	return connect, nil
}

func decodeConnack(data []byte, _ packets.FixedHeader) packets.MqttPacket {
	//first byte is weird in connack
	bookmark := uint32(1)
	retcode := data[bookmark]

	return &packets.ConnAckPacket{
		ReturnCode: retcode,
	}
}

func decodePublish(data []byte, hdr packets.FixedHeader) (packets.MqttPacket, error) {
	bookmark := uint32(0)
	topic, err := readString(data, &bookmark)
	if err != nil {
		return nil, err
	}
	var msgID uint16
	if hdr.Qos > 0 {
		msgID = readUint16(data, &bookmark)
	}

	return &packets.PublishPacket{
		Header:    hdr,
		TopicName: topic,
		Payload:   data[bookmark:],
		MessageID: msgID,
	}, nil
}

func decodePuback(data []byte) packets.MqttPacket {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &packets.PubAckPacket{
		MessageID: msgID,
	}
}

func decodePubrec(data []byte) packets.MqttPacket {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &packets.PubRecPacket{
		MessageID: msgID,
	}
}

func decodePubrel(data []byte, hdr packets.FixedHeader) packets.MqttPacket {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &packets.PubRelPacket{
		Header:    hdr,
		MessageID: msgID,
	}
}

func decodePubcomp(data []byte) packets.MqttPacket {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &packets.PubCompPacket{
		MessageID: msgID,
	}
}

func decodeSubscribe(data []byte, hdr packets.FixedHeader) (packets.MqttPacket, error) {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	var topics []packets.TopicQosTuple
	maxlen := uint32(len(data))
	var err error
	for bookmark < maxlen {
		var t packets.TopicQosTuple
		t.Topic, err = readString(data, &bookmark)
		if err != nil {
			return nil, err
		}
		qos := data[bookmark]
		bookmark++
		t.Qos = uint8(qos)
		topics = append(topics, t)
	}
	return &packets.SubscribePacket{
		Header:        hdr,
		MessageID:     msgID,
		Subscriptions: topics,
	}, nil
}

func decodeSuback(data []byte) packets.MqttPacket {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	var qoses []uint8
	maxlen := uint32(len(data))
	//is this efficient
	for bookmark < maxlen {
		qos := data[bookmark]
		bookmark++
		qoses = append(qoses, qos)
	}
	return &packets.SubAckPacket{
		MessageID: msgID,
		Qos:       qoses,
	}
}

func decodeUnsubscribe(data []byte, hdr packets.FixedHeader) (packets.MqttPacket, error) {
	bookmark := uint32(0)
	var topics []packets.TopicQosTuple
	msgID := readUint16(data, &bookmark)
	maxlen := uint32(len(data))
	var err error
	for bookmark < maxlen {
		var t packets.TopicQosTuple
		//		qos := data[bookmark]
		//		bookmark++
		t.Topic, err = readString(data, &bookmark)
		if err != nil {
			return nil, err
		}
		//		t.qos = uint8(qos)
		topics = append(topics, t)
	}
	return &packets.Unsubscribe{
		Header:    hdr,
		MessageID: msgID,
		Topics:    topics,
	}, nil
}

func decodeUnsuback(data []byte) packets.MqttPacket {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &packets.UnsubAckPacket{
		MessageID: msgID,
	}
}

func decodePingreq() packets.MqttPacket {
	return &packets.PingReq{}
}

func decodePingresp() packets.MqttPacket {
	return &packets.PingResp{}
}

func decodeDisconnect() packets.MqttPacket {
	return &packets.DisconnectPacket{}
}

func readString(b []byte, startsAt *uint32) ([]byte, error) {
	l := readUint16(b, startsAt)
	if uint32(l)+*startsAt > uint32(len(b)) {
		return nil, packets.ErrMessageBadPacket
	}
	v := b[*startsAt : uint32(l)+*startsAt]
	*startsAt += uint32(l)
	return v, nil
}

func readUint16(b []byte, startsAt *uint32) uint16 {
	b0 := uint16(b[*startsAt])
	b1 := uint16(b[*startsAt+1])
	*startsAt += 2

	return (b0 << 8) + b1
}
