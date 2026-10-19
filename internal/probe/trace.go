package probe

import (
	"crypto/tls"
	"encoding/json"
	"net/http/httptrace"
	"sync"
	"time"
)

// Timings contains completed phase durations; phases can overlap and do not sum to Total.
type Timings struct {
	DNS       *float64 `json:"dns,omitempty"`
	Connect   *float64 `json:"connect,omitempty"`
	TLS       *float64 `json:"tls,omitempty"`
	FirstByte *float64 `json:"first_byte,omitempty"`
	Total     float64  `json:"total"`
}

// String returns the same safe timing labels as the JSON report.
func (t Timings) String() string {
	data, _ := json.Marshal(t)
	return string(data)
}

type traceState struct {
	mu      sync.Mutex
	start   time.Time
	dns     time.Time
	tls     time.Time
	connect map[string][]time.Time
	timings Timings
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func addTiming(value **float64, start time.Time) {
	if start.IsZero() {
		return
	}
	if *value == nil {
		*value = new(float64)
	}
	**value += milliseconds(time.Since(start))
}

func (s *traceState) callbacks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { s.mu.Lock(); defer s.mu.Unlock(); s.dns = time.Now() },
		DNSDone: func(httptrace.DNSDoneInfo) {
			s.mu.Lock()
			defer s.mu.Unlock()
			addTiming(&s.timings.DNS, s.dns)
			s.dns = time.Time{}
		},
		ConnectStart: func(network, addr string) {
			s.mu.Lock()
			defer s.mu.Unlock()
			key := network + "\x00" + addr
			s.connect[key] = append(s.connect[key], time.Now())
		},
		ConnectDone: func(network, addr string, _ error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			key := network + "\x00" + addr
			if starts := s.connect[key]; len(starts) > 0 {
				addTiming(&s.timings.Connect, starts[0])
				s.connect[key] = starts[1:]
			}
		},
		TLSHandshakeStart: func() { s.mu.Lock(); defer s.mu.Unlock(); s.tls = time.Now() },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			addTiming(&s.timings.TLS, s.tls)
			s.tls = time.Time{}
		},
		GotFirstResponseByte: func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			value := milliseconds(time.Since(s.start))
			s.timings.FirstByte = &value
		},
	}
}

func (s *traceState) snapshot() Timings {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.timings
	// Copy pointers because transport callbacks can finish after a failed request returns.
	for _, field := range []**float64{&result.DNS, &result.Connect, &result.TLS, &result.FirstByte} {
		if *field != nil {
			value := **field
			*field = &value
		}
	}
	result.Total = milliseconds(time.Since(s.start))
	return result
}
