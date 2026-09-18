package hydracore

import "sync/atomic"

var networkGeneration atomic.Uint64

func SetNetworkGeneration(generation uint64) {
	networkGeneration.Store(generation)
}

func CurrentNetworkGeneration() uint64 {
	return networkGeneration.Load()
}
