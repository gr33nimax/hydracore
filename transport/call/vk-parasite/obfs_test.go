package vkparasite

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRTPCodecPaddingDistribution(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	receiver, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := []byte("test-rtp-payload-distribution-check")
	counts := make(map[int]int, defaultRTPPadding)
	const iterations = 10000

	for i := 0; i < iterations; i++ {
		wire, raw, err := codec.wrap(payload)
		require.NoError(t, err)
		require.Greater(t, len(wire), rtpHeaderLength+len(payload))

		paddingLength := int(wire[len(wire)-1])
		require.GreaterOrEqual(t, paddingLength, 1)
		require.LessOrEqual(t, paddingLength, defaultRTPPadding)

		counts[paddingLength]++

		plain, err := receiver.unwrap(nil, wire)
		require.NoError(t, err)
		require.True(t, bytes.Equal(plain, payload))

		codec.putBuffer(raw)
	}

	require.Equal(t, defaultRTPPadding, len(counts), "all padding lengths in range 1..24 must occur")
	for padLen := 1; padLen <= defaultRTPPadding; padLen++ {
		require.Greater(t, counts[padLen], 0, "padLen %d should have non-zero occurrences", padLen)
	}
}

func TestRTPCodecPoolBufferReuse(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	for i := range key {
		key[i] = byte(i + 42)
	}
	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := []byte("hello-pool-reuse")
	wire1, raw1, err := codec.wrap(payload)
	require.NoError(t, err)
	require.NotNil(t, raw1)
	codec.putBuffer(raw1)

	wire2, raw2, err := codec.wrap(payload)
	require.NoError(t, err)
	require.NotNil(t, raw2)
	codec.putBuffer(raw2)
	_ = wire1
	_ = wire2
}

func TestRTPCodecLargePayloadFallback(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	for i := range key {
		key[i] = byte(i + 13)
	}
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	receiver, err := newRTPCodec(key)
	require.NoError(t, err)

	largePayload := make([]byte, 2048)
	for i := range largePayload {
		largePayload[i] = byte(i)
	}
	wire, raw, err := codec.wrap(largePayload)
	require.NoError(t, err)
	require.Nil(t, raw)
	codec.putBuffer(raw)

	plain, err := receiver.unwrap(nil, wire)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, largePayload))
}

func TestRTPCodecConcurrentWrap(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	for i := range key {
		key[i] = byte(i + 7)
	}
	codec, err := newRTPCodec(key)
	require.NoError(t, err)

	const goroutines = 16
	const perGoroutine = 500
	done := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			payload := []byte("concurrent-payload")
			for i := 0; i < perGoroutine; i++ {
				wire, raw, err := codec.wrap(payload)
				if err != nil {
					done <- err
					return
				}
				_ = wire
				codec.putBuffer(raw)
			}
			done <- nil
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		require.NoError(t, <-done)
	}
}

func TestRTPCodecRFC8285ExtensionAndDynamicPT(t *testing.T) {
	t.Parallel()
	var key [wrapKeyLength]byte
	for i := range key {
		key[i] = byte(i + 9)
	}
	codec, err := newRTPCodec(key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, codec.payloadType, byte(96))
	require.LessOrEqual(t, codec.payloadType, byte(127))

	receiver, err := newRTPCodec(key)
	require.NoError(t, err)

	payload := []byte("rfc-8285-test-payload")
	wire, raw, err := codec.wrap(payload)
	require.NoError(t, err)
	defer codec.putBuffer(raw)

	plain, err := receiver.unwrap(nil, wire)
	require.NoError(t, err)
	require.Equal(t, payload, plain)
}
