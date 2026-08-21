package vkparasite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ai-generated: unit test for MTU budget invariants
func TestMTUBudget(t *testing.T) {
	require.GreaterOrEqual(t, quicPacketSize, quicMinimumPacketSize)
	require.LessOrEqual(t, quicPacketSize+overheadDTLSRecord, dtlsMTU)
	// Обёрнутый пакет обязан влезать в буфер пула из obfs.go.
	require.LessOrEqual(t,
		quicPacketSize+overheadDTLSRecord+overheadRTPHeader+
			overheadRTPAEAD+overheadRTPPadding,
		maxCodecWireBuffer)
	require.LessOrEqual(t, conservativePathMTU-pathOverheadTotal, quicPacketSize+64)
}
