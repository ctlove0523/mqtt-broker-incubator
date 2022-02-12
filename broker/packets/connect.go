package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type ConnectPacket struct {
	ProtoName        []byte
	Version          uint8
	CleanSessionFlag bool
	WillFlag         bool
	WillQOS          uint8
	WillRetainFlag   bool
	UsernameFlag     bool
	PasswordFlag     bool
	KeepAlive        uint16

	// 载荷
	ClientID    []byte
	WillTopic   []byte
	WillMessage []byte
	Username    []byte
	Password    []byte
}

func (c *ConnectPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	// Calculate the max length
	head, buf := array.Split(maxHeaderSize)

	// pack the proto name and version
	offset := writeString(buf, c.ProtoName)
	offset += writeUint8(buf[offset:], c.Version)

	// pack the flags
	var flagByte byte
	flagByte |= boolToUInt8(c.UsernameFlag) << 7
	flagByte |= boolToUInt8(c.PasswordFlag) << 6
	flagByte |= boolToUInt8(c.WillRetainFlag) << 5
	flagByte |= c.WillQOS << 3
	flagByte |= boolToUInt8(c.WillFlag) << 2
	flagByte |= boolToUInt8(c.CleanSessionFlag) << 1

	offset += writeUint8(buf[offset:], flagByte)
	offset += writeUint16(buf[offset:], c.KeepAlive)
	offset += writeString(buf[offset:], c.ClientID)

	if c.WillFlag {
		offset += writeString(buf[offset:], c.WillTopic)
		offset += writeString(buf[offset:], c.WillMessage)
	}

	if c.UsernameFlag {
		offset += writeString(buf[offset:], c.Username)
	}

	if c.PasswordFlag {
		offset += writeString(buf[offset:], c.Password)
	}

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfConnect, nil, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (c *ConnectPacket) Type() uint8 {
	return TypeOfConnect
}

