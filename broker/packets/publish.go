package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type PublishPacket struct {
	Header    FixedHeader
	TopicName []byte
	MessageID uint16
	Payload   []byte
}

func (p *PublishPacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Pools.Get()
	defer buffers.Pools.Put(array)

	head, buf := array.Split(maxHeaderSize)
	length := 2 + len(p.TopicName) + len(p.Payload)
	if p.Header.Qos > 0 {
		length += 2
	}

	if length > maxMessageSize {
		return 0, ErrMessageTooLarge
	}

	// Write the packet
	offset := writeString(buf, p.TopicName)
	if p.Header.Qos > 0 {
		offset += writeUint16(buf[offset:], p.MessageID)
	}

	copy(buf[offset:], p.Payload)
	offset += len(p.Payload)

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfPublish, &p.Header, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (p *PublishPacket) Type() uint8 {
	return TypeOfPublish
}
