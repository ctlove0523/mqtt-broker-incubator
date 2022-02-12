package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type PubAckPacket struct {
	FixedHeader
	MessageID uint16
}

func (p *PubAckPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, p.MessageID)

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfPubAck, nil, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (p *PubAckPacket) Type() uint8 {
	return TypeOfPubAck
}
