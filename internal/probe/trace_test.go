package probe

import (
	"crypto/tls"
	"net/http/httptrace"
	"sync"
	"testing"
	"time"
)

func TestTraceConcurrentCallbacks(t *testing.T) {
	state := &traceState{start: time.Now(), connect: make(map[string][]time.Time)}
	callbacks := state.callbacks()
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			callbacks.ConnectStart("tcp", "127.0.0.1:1")
			callbacks.ConnectDone("tcp", "127.0.0.1:1", nil)
			callbacks.DNSStart(httptrace.DNSStartInfo{})
			callbacks.DNSDone(httptrace.DNSDoneInfo{})
			callbacks.TLSHandshakeStart()
			callbacks.TLSHandshakeDone(tls.ConnectionState{}, nil)
			callbacks.GotFirstResponseByte()
			_ = state.snapshot().String()
		}()
	}
	group.Wait()
	timings := state.snapshot()
	if timings.Connect == nil || timings.DNS == nil || timings.TLS == nil || timings.FirstByte == nil || timings.Total <= 0 {
		t.Fatalf("missing completed phases: %+v", timings)
	}
}

func TestTraceSnapshotRemainsStableAfterLaterCallbacks(t *testing.T) {
	state := &traceState{start: time.Now(), connect: make(map[string][]time.Time)}
	callbacks := state.callbacks()
	initial := state.snapshot()
	if initial.DNS != nil || initial.Connect != nil || initial.TLS != nil || initial.FirstByte != nil {
		t.Fatalf("unmeasured phases were populated: %+v", initial)
	}
	callbacks.ConnectDone("tcp", "unmatched", nil)
	callbacks.DNSDone(httptrace.DNSDoneInfo{})
	callbacks.TLSHandshakeDone(tls.ConnectionState{}, nil)
	if state.snapshot().Connect != nil || state.snapshot().DNS != nil || state.snapshot().TLS != nil {
		t.Fatal("completion without start created a measurement")
	}
	callbacks.ConnectStart("tcp", "address")
	callbacks.ConnectDone("tcp", "address", nil)
	callbacks.DNSStart(httptrace.DNSStartInfo{})
	callbacks.DNSDone(httptrace.DNSDoneInfo{})
	callbacks.TLSHandshakeStart()
	callbacks.TLSHandshakeDone(tls.ConnectionState{}, nil)
	callbacks.GotFirstResponseByte()
	frozen := state.snapshot()
	encoded := frozen.String()
	var group sync.WaitGroup
	for range 10 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 20 {
				callbacks.ConnectStart("tcp", "address")
				callbacks.ConnectDone("tcp", "address", nil)
				callbacks.DNSStart(httptrace.DNSStartInfo{})
				callbacks.DNSDone(httptrace.DNSDoneInfo{})
				callbacks.TLSHandshakeStart()
				callbacks.TLSHandshakeDone(tls.ConnectionState{}, nil)
				callbacks.GotFirstResponseByte()
				if frozen.String() != encoded {
					t.Error("snapshot changed after later callbacks")
					return
				}
			}
		}()
	}
	group.Wait()
	if frozen.String() != encoded {
		t.Fatal("snapshot aliases mutable callback state")
	}
}
