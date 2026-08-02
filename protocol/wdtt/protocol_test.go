package wdtt

import (
	"errors"
	"net"
	"testing"

	"github.com/gr33nimax/hydra-wdtt/pkg/control"
)

func TestSendHydraAuthWaitsForServerAcceptance(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverErr := make(chan error, 1)
	go func() {
		buffer := make([]byte, 4096)
		n, err := server.Read(buffer)
		if err != nil {
			serverErr <- err
			return
		}
		operation, request, matched, err := control.DecodeRequest(buffer[:n])
		if err != nil || !matched || operation != control.OperationAuth || request.Workers != 18 ||
			request.RuntimeID != "runtime-1" || request.Generation != 3 || request.WorkerSlot != 7 {
			serverErr <- errors.New("unexpected Hydra auth request")
			return
		}
		acknowledgement, err := control.EncodeLease(control.Response{Features: []string{control.FeatureLease}})
		if err == nil {
			_, err = server.Write(acknowledgement)
		}
		serverErr <- err
	}()

	if err := sendHydraAuth(client, "device-1", "wdtt:user-1:device-1", "lease-token", 18, "runtime-1", 3, 7); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
