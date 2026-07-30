package xhttp

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitReadCloserCloseBeforeSetIsTerminal(t *testing.T) {
	waitReader := &WaitReadCloser{Wait: make(chan struct{})}
	reader := &countingReadCloser{}

	if err := waitReader.Close(); err != nil {
		t.Fatal(err)
	}
	waitReader.Set(reader)
	if err := waitReader.Close(); err != nil {
		t.Fatal(err)
	}

	if reader.closeCount.Load() != 1 {
		t.Fatalf("reader closed %d times, want 1", reader.closeCount.Load())
	}
	if _, err := waitReader.Read(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read error = %v, want %v", err, io.ErrClosedPipe)
	}
}

func TestWaitReadCloserSetCloseRace(t *testing.T) {
	const iterations = 1000

	for range iterations {
		waitReader := &WaitReadCloser{Wait: make(chan struct{})}
		reader := &countingReadCloser{}
		start := make(chan struct{})
		readResult := make(chan error, 1)
		var workers sync.WaitGroup
		workers.Add(3)

		go func() {
			defer workers.Done()
			<-start
			waitReader.Set(reader)
		}()
		go func() {
			defer workers.Done()
			<-start
			_ = waitReader.Close()
		}()
		go func() {
			defer workers.Done()
			<-start
			_, err := waitReader.Read(nil)
			readResult <- err
		}()

		close(start)
		workers.Wait()
		if err := waitReader.Close(); err != nil {
			t.Fatal(err)
		}
		if reader.closeCount.Load() != 1 {
			t.Fatalf("reader closed %d times, want 1", reader.closeCount.Load())
		}
		if err := <-readResult; !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Read error = %v, want EOF or %v", err, io.ErrClosedPipe)
		}
	}
}

func TestWaitReadCloserClosesReaderWithoutHoldingStateLock(t *testing.T) {
	waitReader := &WaitReadCloser{Wait: make(chan struct{})}
	readerClosed := make(chan struct{})
	waitReader.Set(readCloserFunc{
		close: func() error {
			if err := waitReader.Close(); err != nil {
				return err
			}
			close(readerClosed)
			return nil
		},
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- waitReader.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("underlying Close called while WaitReadCloser state lock was held")
	}
	select {
	case <-readerClosed:
	default:
		t.Fatal("underlying reader was not closed")
	}
}

type countingReadCloser struct {
	closeCount atomic.Int32
}

func (*countingReadCloser) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (r *countingReadCloser) Close() error {
	r.closeCount.Add(1)
	return nil
}

type readCloserFunc struct {
	close func() error
}

func (readCloserFunc) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (r readCloserFunc) Close() error {
	return r.close()
}
