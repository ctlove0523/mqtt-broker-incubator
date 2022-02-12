package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type SubscribePacket struct {
	Header FixedHeader
	MessageID     uint16
	Subscriptions []TopicQosTuple
}

func (s *SubscribePacket) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, s.MessageID)
	for _, t := range s.Subscriptions {
		offset += writeString(buf[offset:], t.Topic)
		offset += writeUint8(buf[offset:], byte(t.Qos))
	}

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfSubscribe, &s.Header, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (s *SubscribePacket) Type() uint8 {
	return TypeOfSubscribe
}
