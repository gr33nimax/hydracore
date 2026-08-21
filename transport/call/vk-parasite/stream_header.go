package vkparasite

import (
	"errors"
	"fmt"
	"io"

	M "github.com/sagernet/sing/common/metadata"
)

const (
	streamKindTCP byte = 0x01
	streamKindUDP byte = 0x02
)

var (
	errUnknownStreamKind = errors.New("call vk_parasite: unknown stream kind")
	errInvalidAddress    = errors.New("call vk_parasite: invalid stream destination address")
)

func writeStreamHeader(w io.Writer, kind byte, dest M.Socksaddr) error {
	if kind != streamKindTCP && kind != streamKindUDP {
		return fmt.Errorf("%w: %d", errUnknownStreamKind, kind)
	}
	if !dest.IsValid() {
		return errInvalidAddress
	}
	if _, err := w.Write([]byte{kind}); err != nil {
		return err
	}
	return M.SocksaddrSerializer.WriteAddrPort(w, dest)
}

func readStreamHeader(r io.Reader) (byte, M.Socksaddr, error) {
	var kindBuf [1]byte
	if _, err := io.ReadFull(r, kindBuf[:]); err != nil {
		return 0, M.Socksaddr{}, err
	}
	kind := kindBuf[0]
	if kind != streamKindTCP && kind != streamKindUDP {
		return 0, M.Socksaddr{}, fmt.Errorf("%w: %d", errUnknownStreamKind, kind)
	}
	dest, err := M.SocksaddrSerializer.ReadAddrPort(r)
	if err != nil {
		return 0, M.Socksaddr{}, err
	}
	return kind, dest, nil
}
