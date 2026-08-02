// SPDX-License-Identifier: GPL-3.0-only

package wdtt

import (
	"context"
	"net"
	"sync"
	"time"
)

const (
	packetBufferSize    = 2048
	workerSendQueueSize = 128
	returnQueueSize     = 384
	dispatchChunkSize   = 8
)

var packetPool = sync.Pool{New: func() any { return make([]byte, packetBufferSize) }}

func acquirePacket(size int) []byte {
	buffer := packetPool.Get().([]byte)
	if cap(buffer) < size {
		return make([]byte, size)
	}
	return buffer[:size]
}

func releasePacket(buffer []byte) {
	if cap(buffer) >= packetBufferSize {
		packetPool.Put(buffer[:packetBufferSize])
	}
}

type workerSlot struct {
	id         int
	generation int
	sendCh     chan []byte
}

type dispatcher struct {
	ctx       context.Context
	cancel    context.CancelFunc
	localConn net.PacketConn
	returnCh  chan []byte

	mu         sync.Mutex
	workers    []*workerSlot
	active     []*workerSlot
	activeGeneration int
	clientAddr net.Addr
	roundRobin int
	chunkCount int
	waitGroup  sync.WaitGroup
}

func newDispatcher(ctx context.Context, localConn net.PacketConn) *dispatcher {
	dispatchContext, cancel := context.WithCancel(ctx)
	d := &dispatcher{
		ctx:       dispatchContext,
		cancel:    cancel,
		localConn: localConn,
		returnCh:  make(chan []byte, returnQueueSize),
		activeGeneration: 1,
	}
	d.waitGroup.Add(2)
	go d.readLoop()
	go d.writeLoop()
	return d
}

func (d *dispatcher) close() {
	d.cancel()
	_ = d.localConn.SetDeadline(time.Now())
	d.waitGroup.Wait()
}

func (d *dispatcher) register(id int, generation int) *workerSlot {
	slot := &workerSlot{id: id, generation: generation, sendCh: make(chan []byte, workerSendQueueSize)}
	d.mu.Lock()
	d.workers = append(d.workers, slot)
	if generation == d.activeGeneration {
		d.active = append(d.active, slot)
	}
	d.mu.Unlock()
	return slot
}

func (d *dispatcher) activateGeneration(generation int) {
	d.mu.Lock()
	d.activeGeneration = generation
	d.active = d.active[:0]
	for _, slot := range d.workers {
		if slot.generation == generation {
			d.active = append(d.active, slot)
		}
	}
	d.roundRobin = 0
	d.chunkCount = 0
	d.mu.Unlock()
}

func (d *dispatcher) unregister(slot *workerSlot) {
	d.mu.Lock()
	for index, candidate := range d.workers {
		if candidate == slot {
			d.workers = append(d.workers[:index], d.workers[index+1:]...)
			break
		}
	}
	for index, candidate := range d.active {
		if candidate == slot {
			d.active = append(d.active[:index], d.active[index+1:]...)
			break
		}
	}
	if len(d.active) == 0 {
		d.roundRobin = 0
	} else if d.roundRobin >= len(d.active) {
		d.roundRobin %= len(d.active)
	}
	d.chunkCount = 0
	d.mu.Unlock()
}

func (d *dispatcher) readLoop() {
	defer d.waitGroup.Done()
	buffer := make([]byte, packetBufferSize)
	for {
		n, address, err := d.localConn.ReadFrom(buffer)
		if err != nil {
			if d.ctx.Err() != nil {
				return
			}
			continue
		}
		packet := acquirePacket(n)
		copy(packet, buffer[:n])

		d.mu.Lock()
		if d.clientAddr == nil {
			d.clientAddr = address
		} else if d.clientAddr.Network() != address.Network() || d.clientAddr.String() != address.String() {
			d.mu.Unlock()
			releasePacket(packet)
			continue
		}
		workerCount := len(d.active)
		if workerCount == 0 {
			d.mu.Unlock()
			releasePacket(packet)
			continue
		}
		index := d.roundRobin % workerCount
		sent := false
		select {
		case d.active[index].sendCh <- packet:
			sent = true
			d.chunkCount++
			if d.chunkCount >= dispatchChunkSize {
				d.roundRobin = (index + 1) % workerCount
				d.chunkCount = 0
			}
		default:
			for offset := 1; offset < workerCount; offset++ {
				alternate := (index + offset) % workerCount
				select {
				case d.active[alternate].sendCh <- packet:
					sent = true
					d.roundRobin = alternate
					d.chunkCount = 1
				default:
				}
				if sent {
					break
				}
			}
		}
		if !sent {
			d.roundRobin = (index + 1) % workerCount
			d.chunkCount = 0
		}
		d.mu.Unlock()
		if !sent {
			releasePacket(packet)
		}
	}
}

func (d *dispatcher) writeLoop() {
	defer d.waitGroup.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case packet := <-d.returnCh:
			d.mu.Lock()
			address := d.clientAddr
			d.mu.Unlock()
			if address != nil {
				_, _ = d.localConn.WriteTo(packet, address)
			}
			releasePacket(packet)
		}
	}
}
