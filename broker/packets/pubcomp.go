package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type PubCompPacket struct {
	FixedHeader
	MessageID uint16
}

func (p *PubCompPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Pools.Get()
	defer buffers.Pools.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, p.MessageID)

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfPubComp, nil, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (p *PubCompPacket) Type() uint8 {
	return TypeOfPubComp
}
