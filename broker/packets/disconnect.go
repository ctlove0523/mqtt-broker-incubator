package packets

import "io"

type DisconnectPacket struct {
	FixedHeader
}

func (d *DisconnectPacket) EncodeTo(w io.Writer) (int, error) {
	return w.Write([]byte{0xe0, 0x0})
}

func (d *DisconnectPacket) Type() uint8 {
	return TypeOfDisconnect
}
