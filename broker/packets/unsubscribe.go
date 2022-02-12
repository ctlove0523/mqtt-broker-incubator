package packets

import (
	"github.com/ctlove0523/mqtt-brokers/broker/buffers"
	"io"
)

type Unsubscribe struct {
	Header FixedHeader
	MessageID uint16
	Topics    []TopicQosTuple
}

func (u *Unsubscribe) EncodeTo(w io.Writer) (int, error) {
	array := buffers.Buffers.Get()
	defer buffers.Buffers.Put(array)

	head, buf := array.Split(maxHeaderSize)
	offset := writeUint16(buf, u.MessageID)
	for _, toptup := range u.Topics {
		offset += writeString(buf[offset:], toptup.Topic)
	}

	// Write the header in front and return the buffer
	start := writeHeader(head, TypeOfUnsubscribe, &u.Header, offset)
	return w.Write(array.Slice(start, maxHeaderSize+offset))
}

func (u *Unsubscribe) Type() uint8 {
	return TypeOfUnsubscribe
}
