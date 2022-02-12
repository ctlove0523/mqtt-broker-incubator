package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type UnsubAckPacket struct {
	FixedHeader
	MessageID uint16
}

func (u *UnsubAckPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, u.MessageID)

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfUnsubAck, nil, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (u *UnsubAckPacket) Type() uint8 {
	return TypeOfUnsubAck
}
