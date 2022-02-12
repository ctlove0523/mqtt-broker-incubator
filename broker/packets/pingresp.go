package packets

import "io"

type PingResp struct {
	FixedHeader
}

func (p *PingResp) EncodeTo(w io.Writer) (int, error) {
	return w.Write([]byte{0xd0, 0x0})
}

func (p *PingResp) Type() uint8 {
	return TypeOfPingResp
}
