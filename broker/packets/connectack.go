package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

// ConnAckPacket represents an MQTT connack packet.
// 0x00 connection accepted
// 0x01 refused: unacceptable proto version
// 0x02 refused: identifier rejected
// 0x03 refused server unavailiable
// 0x04 bad user or password
// 0x05 not authorized
type ConnAckPacket struct {
	FixedHeader
	SessionPresent bool
	ReturnCode     byte
}

func (c *ConnAckPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Pools.Get()
	defer buffers.Pools.Put(array)

	//write padding
	head, buf := array.Split(maxHeaderSize)
	offset := writeUint8(buf, byte(0))
	offset += writeUint8(buf[offset:], byte(c.ReturnCode))

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfConnAck, nil, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (c *ConnAckPacket) Type() uint8 {
	return TypeOfConnAck
}
