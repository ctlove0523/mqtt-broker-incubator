package packets

import "io"

type PingReq struct {
	FixedHeader
}

func (p *PingReq) EncodeTo(w io.Writer) (int, error) {
	return w.Write([]byte{0xc0, 0x0})
}

func (p *PingReq) Type() uint8 {
	return TypeOfPingReq
}

