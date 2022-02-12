package packets

import (
	"errors"
	"io"
)

const (
	maxHeaderSize  = 6     // max MQTT header size
	maxMessageSize = 65536 // max MQTT message size is impossible to increase as per protocol (uint16 len)
)

var ErrMessageTooLarge = errors.New("mqtt: message size exceeds 64K")
var ErrMessageBadPacket = errors.New("mqtt: bad packet")

// MQTT控制包类型
// MQTT message types
// 名字      值  报文方向         描述
// Reserved	0	禁止	保留
// CONNECT	1	客户端到服务端	客户端请求连接服务端
// CONNACK	2	服务端到客户端	连接报文确认
// PUBLISH	3	两个方向都允许	发布消息
// PUBACK	4	两个方向都允许	QoS 1消息发布收到确认
// PUBREC	5	两个方向都允许	发布收到（保证交付第一步）
// PUBREL	6	两个方向都允许	发布释放（保证交付第二步）
// PUBCOMP	7	两个方向都允许	QoS 2消息发布完成（保证交互第三步）
// SUBSCRIBE	8	客户端到服务端	客户端订阅请求
// SUBACK	9	服务端到客户端	订阅请求报文确认
// UNSUBSCRIBE	10	客户端到服务端	客户端取消订阅请求
// UNSUBACK	11	服务端到客户端	取消订阅报文确认
// PINGREQ	12	客户端到服务端	心跳请求
// PINGRESP	13	服务端到客户端	心跳响应
// DISCONNECT	14	两个方向都允许	断开连接通知
// AUTH	15	两个方向都允许	认证信息交换
const (
	TypeOfConnect = uint8(iota + 1)
	TypeOfConnAck
	TypeOfPublish
	TypeOfPubAck
	TypeOfPubRec
	TypeOfPubRel
	TypeOfPubComp
	TypeOfSubscribe
	TypeOfSubAck
	TypeOfUnsubscribe
	TypeOfUnsubAck
	TypeOfPingReq
	TypeOfPingResp
	TypeOfDisconnect
)

type TopicQosTuple struct {
	FixedHeader
	Qos   uint8
	Topic []byte
}

type MqttPacket interface {
	Type() uint8
	EncodeTo(w io.Writer) (int, error)
}

type FixedHeader struct {
	MessageType     byte
	Dup             bool
	Qos             byte
	Retain          bool
	RemainingLength int
}


// encodeParts sews the whole packet together
func writeHeader(buf []byte, msgType uint8, h *FixedHeader, length int) int {
	var firstByte byte
	firstByte |= msgType << 4
	if h != nil {
		firstByte |= boolToUInt8(h.Dup) << 3
		firstByte |= h.Qos << 1
		firstByte |= boolToUInt8(h.Retain)
	}

	// get the length first
	numBytes, bitField := encodeLength(uint32(length))
	offset := 6 - numBytes - 1 //to account for the first byte

	// now we blit it in
	buf[offset] = byte(firstByte)
	for i := offset + 1; i < 6; i++ {
		buf[i] = byte(bitField >> ((numBytes - 1) * 8))
		numBytes--
	}

	return int(offset)
}

func writeString(buf, v []byte) int {
	length := len(v)
	writeUint16(buf, uint16(length))
	copy(buf[2:], v)
	return 2 + length
}

func writeUint16(buf []byte, v uint16) int {
	buf[0] = byte((v & 0xff00) >> 8)
	buf[1] = byte(v & 0x00ff)
	return 2
}

func writeUint8(buf []byte, v uint8) int {
	buf[0] = v
	return 1
}

func readString(b []byte, startsAt *uint32) ([]byte, error) {
	l := readUint16(b, startsAt)
	if uint32(l)+*startsAt > uint32(len(b)) {
		return nil, ErrMessageBadPacket
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

func boolToUInt8(v bool) uint8 {
	if v {
		return 0x1
	}

	return 0x0
}

// encodeLength encodes the length formatting (see: http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#fixed-header)
// and tells us how many bytes it takes up.
func encodeLength(bodyLength uint32) (uint8, uint32) {
	if bodyLength == 0 {
		return 1, 0
	}

	var bitField uint32
	var numBytes uint8
	for bodyLength > 0 {
		bitField <<= 8
		dig := uint8(bodyLength % 128)
		bodyLength /= 128
		if bodyLength > 0 {
			dig = dig | 0x80
		}

		bitField |= uint32(dig)
		numBytes++
	}
	return numBytes, bitField
}
