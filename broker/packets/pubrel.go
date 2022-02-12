package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type PubRelPacket struct {
	Header FixedHeader
	MessageID uint16
}

func (p *PubRelPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, p.MessageID)

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfPubRel, &p.Header, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (p *PubRelPacket) Type() uint8 {
	return TypeOfPubRel
}