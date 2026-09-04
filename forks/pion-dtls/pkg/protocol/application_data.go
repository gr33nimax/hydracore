// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package protocol

import "sync"

// pooledApplicationDataSize is the buffer an inbound record's payload is kept in.
//
// A DTLS application record on a datagram transport is bounded by the path MTU, so this covers
// every real record with room to spare. Anything larger falls back to a plain allocation and
// never enters the pool, which is why Release checks the capacity rather than the length.
const pooledApplicationDataSize = 2048

var applicationDataPool = sync.Pool{
	New: func() any {
		buffer := make([]byte, pooledApplicationDataSize)

		return &buffer
	},
}

// ReleaseApplicationData hands an unmarshalled payload back for reuse.
//
// Best-effort by design: a payload that is never released is simply collected as before, so a
// caller that holds on to Data cannot be corrupted by this. Only release once the payload has
// been consumed and will not be read again.
func ReleaseApplicationData(data []byte) {
	if cap(data) != pooledApplicationDataSize {
		return
	}
	full := data[:pooledApplicationDataSize]
	applicationDataPool.Put(&full)
}

// ApplicationData messages are carried by the record layer and are
// fragmented, compressed, and encrypted based on the current connection
// state.  The messages are treated as transparent data to the record
// layer.
// https://tools.ietf.org/html/rfc5246#section-10
type ApplicationData struct {
	Data []byte
}

// ContentType returns the ContentType of this content.
func (a ApplicationData) ContentType() ContentType {
	return ContentTypeApplicationData
}

// Marshal encodes the ApplicationData to binary.
func (a *ApplicationData) Marshal() ([]byte, error) {
	return append([]byte{}, a.Data...), nil
}

// Unmarshal populates the ApplicationData from binary.
//
// The copy is required — `data` is the connection's read buffer and this payload outlives the
// call — but it comes from a pool rather than from a fresh allocation on every record. Give it
// back with ReleaseApplicationData once the payload has been consumed.
func (a *ApplicationData) Unmarshal(data []byte) error {
	if len(data) > pooledApplicationDataSize {
		a.Data = append([]byte{}, data...)

		return nil
	}
	buffer := (*applicationDataPool.Get().(*[]byte))[:len(data)]
	copy(buffer, data)
	a.Data = buffer

	return nil
}
