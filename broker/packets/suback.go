package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type SubAckPacket struct {
	Header    FixedHeader
	MessageID uint16
	Qos       []uint8
}

func (s *SubAckPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, s.MessageID)
	for _, q := range s.Qos {
		offset += writeUint8(buf[offset:], byte(q))
	}

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfSubAck, &s.Header, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (s *SubAckPacket) Type() uint8 {
	return TypeOfSubAck
}
