/**********************************************************************************
* Copyright (c) 2009-2019 Misakai Ltd.
* This program is free software: you can redistribute it and/or modify it under the
* terms of the GNU Affero General Public License as published by the  Free Software
* Foundation, either version 3 of the License, or(at your option) any later version.
*
* This program is distributed  in the hope that it  will be useful, but WITHOUT ANY
* WARRANTY;  without even  the implied warranty of MERCHANTABILITY or FITNESS FOR A
* PARTICULAR PURPOSE.  See the GNU Affero General Public License  for  more details.
*
* You should have  received a copy  of the  GNU Affero General Public License along
* with this program. If not, see<http://www.gnu.org/licenses/>.
************************************************************************************/

package buffers

import (
	"sync"
)

const (
	// smallBufferSize is an initial allocation minimal capacity.
	smallBufferSize = 64
	maxInt          = int(^uint(0) >> 1)
	maxMessageSize  = 65536 // max MQTT message size is impossible to increase as per protocol (uint16 len)
)

// Buffers are reusable fixed-side Buffers for faster encoding.
var Buffers = NewBufferPool(maxMessageSize)

// BufferPool represents a thread safe buffer pool
type BufferPool struct {
	sync.Pool
}

// NewBufferPool creates a new BufferPool bounded to the given size.
func NewBufferPool(bufferSize int) (bp *BufferPool) {
	return &BufferPool{
		sync.Pool{
			New: func() interface{} {
				return &ByteBuffer{buf: make([]byte, bufferSize)}
			},
		},
	}
}

// Get gets a Buffer from the SizedBufferPool, or creates a new one if none are
// available in the pool. Buffers have a pre-allocated capacity.
func (bp *BufferPool) Get() *ByteBuffer {
	return bp.Pool.Get().(*ByteBuffer)
}

// Put returns the given Buffer to the SizedBufferPool.
func (bp *BufferPool) Put(b *ByteBuffer) {
	bp.Pool.Put(b)
}

type ByteBuffer struct {
	buf []byte
}

// Bytes gets a byte slice of a specified size.
func (b *ByteBuffer) Bytes(n int) []byte {
	if n == 0 { // Return max size
		return b.buf
	}

	return b.buf[:n]
}

// Slice returns a slice at an offset.
func (b *ByteBuffer) Slice(from, until int) []byte {
	return b.buf[from:until]
}

// Split splits the bufer in two.
func (b *ByteBuffer) Split(n int) ([]byte, []byte) {
	buffer := b.Bytes(0)
	return buffer[:n], buffer[n:]
}
