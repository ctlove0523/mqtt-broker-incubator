package buffers

import (
	"sync"
)

const (
	maxMessageSize = 65536
)

var Pools = NewBufferPool(maxMessageSize)

type ByteBufPoll struct {
	sync.Pool
}

func NewBufferPool(bufferSize int) (bp *ByteBufPoll) {
	return &ByteBufPoll{
		sync.Pool{
			New: func() interface{} {
				return &ByteBuf{buf: make([]byte, bufferSize)}
			},
		},
	}
}

func (bp *ByteBufPoll) Get() *ByteBuf {
	return bp.Pool.Get().(*ByteBuf)
}

func (bp *ByteBufPoll) Put(b *ByteBuf) {
	bp.Pool.Put(b)
}

type ByteBuf struct {
	buf []byte
}

func (b *ByteBuf) Bytes(n int) []byte {
	if n <= 0 {
		return b.buf
	}

	return b.buf[:n]
}

func (b *ByteBuf) Slice(begin, end int) []byte {
	return b.buf[begin:end]
}

func (b *ByteBuf) Split(n int) ([]byte, []byte) {
	buffer := b.Bytes(0)
	return buffer[:n], buffer[n:]
}
