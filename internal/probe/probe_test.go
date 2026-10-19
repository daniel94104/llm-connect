package probe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const completed = `{"id":"resp_test","object":"response","status":"completed","error":null,"output":[{"type":"reasoning"},{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"O界"}]}],"future_field":true}`

func config(endpoint string) Config {
	return Config{Endpoint: endpoint, Model: "chosen-model", APIKey: "fake-unique-key", Timeout: time.Second, MaxAttempts: 2}
}

func TestCompletedRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer fake-unique-key" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" || r.ProtoMajor != 1 {
			t.Errorf("unexpected request metadata")
		}
		if r.Header.Get("Idempotency-Key") != "" || r.Header.Get("X-Idempotency-Key") != "" {
			t.Error("unexpected idempotency header")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body) != 5 || body["model"] != "chosen-model" || body["input"] != "Reply with the single word OK." || body["max_output_tokens"] != float64(64) || body["store"] != false || body["stream"] != false {
			t.Errorf("unexpected request body: %v", body)
		}
		w.Header().Set("x-request-id", "req_123-A")
		io.WriteString(w, completed)
	}))
	defer server.Close()
	report, err := Run(context.Background(), config(server.URL+"/v1/responses"))
	if err != nil || !report.OK || report.Class != "success" || report.OutputChars != 2 || len(report.Attempts) != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	a := report.Attempts[0]
	if a.RequestID != "req_123-A" || a.TimingsMS.Connect == nil || a.TimingsMS.FirstByte == nil || a.TimingsMS.DNS != nil || a.TimingsMS.TLS != nil || a.TimingsMS.Total <= 0 || report.TotalMS < a.TimingsMS.Total {
		t.Fatalf("unexpected timings: %+v", report)
	}
}

func TestConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing model", func(c *Config) { c.Model = "" }},
		{"model controls", func(c *Config) { c.Model = "model\n" }},
		{"missing key", func(c *Config) { c.APIKey = "" }},
		{"key whitespace", func(c *Config) { c.APIKey = "secret key" }},
		{"short timeout", func(c *Config) { c.Timeout = time.Millisecond }},
		{"attempt limit", func(c *Config) { c.MaxAttempts = 4 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := config("https://example.invalid/v1/responses")
			tt.mutate(&c)
			report, err := Run(context.Background(), c)
			if err == nil || report.Class != "configuration" || len(report.Attempts) != 0 {
				t.Fatalf("report=%+v err=%v", report, err)
			}
		})
	}
	for _, endpoint := range []string{"http://localhost/responses", "http://192.168.1.1/responses", "http://127.0.0.1.example/responses", "https://user:secret@example.com/path", "https://example.com/path?", "https://example.com/path#", "https://example.com:0/path", "https://example.com:65536/path", "https://example.com:/path", "http://[::1%25lo0]/path", "https:opaque", "https://example.com/path\n"} {
		if validEndpoint(endpoint) {
			t.Errorf("accepted invalid endpoint %q", endpoint)
		}
	}
	for _, endpoint := range []string{"http://127.0.0.1:1234/v1/responses", "http://[::1]/v1/responses", "https://trusted.example/v1/responses"} {
		if !validEndpoint(endpoint) {
			t.Errorf("rejected valid endpoint %q", endpoint)
		}
	}
}

func TestResponseValidation(t *testing.T) {
	tests := []struct{ name, body, class string }{
		{"completed", completed, "success"},
		{"trailing", completed + ` {}`, "invalid_response"},
		{"null", `null`, "invalid_response"},
		{"wrong type", strings.Replace(completed, `"resp_test"`, `5`, 1), "invalid_response"},
		{"missing id", strings.Replace(completed, `"resp_test"`, `""`, 1), "invalid_response"},
		{"failed", strings.Replace(completed, `"status":"completed"`, `"status":"failed"`, 1), "invalid_response"},
		{"incomplete", strings.Replace(completed, `"status":"completed"`, `"status":"incomplete"`, 1), "incomplete"},
		{"refusal", strings.Replace(completed, `"type":"output_text"`, `"type":"refusal"`, 1), "refusal"},
		{"error", strings.Replace(completed, `"error":null`, `"error":{"message":"private"}`, 1), "invalid_response"},
		{"empty text", strings.Replace(completed, `"text":"O界"`, `"text":" "`, 1), "invalid_response"},
		{"wrong role", strings.Replace(completed, `"role":"assistant"`, `"role":"user"`, 1), "invalid_response"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, _ := validateResponse([]byte(tt.body))
			if class != tt.class {
				t.Fatalf("got %s, want %s", class, tt.class)
			}
		})
	}
}

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		name, body, hint, class string
		status, count           int
	}{
		{"recognized", `{"error":{"code":"rate_limit_exceeded"}}`, "0", "success", 429, 2},
		{"quota code wins", `{"error":{"code":"insufficient_quota","type":"rate_limit_exceeded"}}`, "0", "quota", 429, 1},
		{"quota type wins", `{"error":{"type":"insufficient_quota","code":"rate_limit_exceeded"}}`, "0", "quota", 429, 1},
		{"slow down", `{"error":{"code":"slow_down"}}`, "0", "rate_limit", 429, 1},
		{"slow down code overrides rate-limit type", `{"error":{"code":"slow_down","type":"rate_limit_exceeded"}}`, "0", "rate_limit", 429, 1},
		{"slow down type overrides rate-limit code", `{"error":{"type":"slow_down","code":"rate_limit_exceeded"}}`, "0", "rate_limit", 429, 1},
		{"unknown", `{"error":{"code":"unknown"}}`, "0", "rate_limit", 429, 1},
		{"malformed", `{"error":`, "0", "rate_limit", 429, 1},
		{"huge minimum", `{"error":{"code":"rate_limit_exceeded"}}`, "99999999999999999999999999999", "rate_limit", 429, 1},
		{"budget", `{"error":{"code":"rate_limit_exceeded"}}`, "1", "rate_limit", 429, 1},
		{"provider", `{}`, "0", "provider", 503, 1},
		{"auth", `{}`, "0", "auth", 401, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", tt.hint)
					w.WriteHeader(tt.status)
					io.WriteString(w, tt.body)
					return
				}
				io.WriteString(w, completed)
			}))
			defer server.Close()
			report, _ := Run(context.Background(), config(server.URL))
			if report.Class != tt.class || int(calls.Load()) != tt.count || len(report.Attempts) != tt.count {
				t.Fatalf("report=%+v calls=%d", report, calls.Load())
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		value string
		delay time.Duration
		valid bool
	}{
		{"0", 0, true}, {"3", 3 * time.Second, true}, {"-1", 0, false}, {"bad", 0, false}, {"", 0, false},
		{now.Add(4 * time.Second).Format(http.TimeFormat), 4 * time.Second, true},
		{now.Add(-time.Second).Format(http.TimeFormat), 0, true},
		{"9223372036854775807", time.Duration(maxDuration), true},
		{"999999999999999999999999999999", time.Duration(maxDuration), true},
	} {
		delay, valid := retryAfter(tt.value, now)
		if delay != tt.delay || valid != tt.valid {
			t.Errorf("%q: %v,%v", tt.value, delay, valid)
		}
	}
}

func TestTransportSafety(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/fake-unique-key")
		w.WriteHeader(307)
	}))
	defer redirect.Close()
	report, _ := Run(context.Background(), config(redirect.URL))
	if report.Class != "redirect" || targetCalls.Load() != 0 {
		t.Fatalf("redirect followed: %+v", report)
	}
	var disconnects atomic.Int32
	disconnect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		disconnects.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		connection.Close()
	}))
	defer disconnect.Close()
	report, _ = Run(context.Background(), config(disconnect.URL))
	if report.Class != "network" || disconnects.Load() != 1 {
		t.Fatalf("transport replay: %+v calls=%d", report, disconnects.Load())
	}
}

func TestTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, completed) }))
	defer server.Close()
	report, _ := Run(context.Background(), config(server.URL))
	if report.Class != "tls" {
		t.Fatalf("untrusted TLS: %+v", report)
	}
	original := http.DefaultTransport
	trusted := original.(*http.Transport).Clone()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	trusted.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	http.DefaultTransport = trusted
	t.Cleanup(func() { http.DefaultTransport = original; trusted.CloseIdleConnections() })
	report, err := Run(context.Background(), config(server.URL))
	if err != nil || !report.OK || report.Attempts[0].TimingsMS.TLS == nil {
		t.Fatalf("trusted TLS: %+v %v", report, err)
	}
}

func TestLimitsCancellationAndSecrecy(t *testing.T) {
	for _, tt := range []struct {
		name, class string
		handler     http.HandlerFunc
	}{
		{"large body", "invalid_response", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat("x", maxBodyBytes+1)) }},
		{"body deadline", "deadline", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			// The server may not observe a disconnect until it reads the pending request body.
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
		}},
		{"read failure", "network", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			io.WriteString(w, "short")
		}},
		{"private error", "auth", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-request-id", "fake-unique-key")
			w.WriteHeader(401)
			io.WriteString(w, `{"error":{"message":"fake-unique-key private-response"}}`)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			c := config(server.URL)
			c.Timeout = 100 * time.Millisecond
			report, err := Run(context.Background(), c)
			if err == nil || report.Class != tt.class || len(report.Attempts) != 1 {
				t.Fatalf("%+v %v", report, err)
			}
			encoded, _ := json.Marshal(report)
			if strings.Contains(string(encoded), "fake-unique-key") || strings.Contains(string(encoded), "private-response") || strings.Contains(fmt.Sprint(err), "fake-unique-key") {
				t.Fatalf("private value leaked")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := Run(ctx, config("http://127.0.0.1:1"))
	if err == nil || report.Class != "canceled" || len(report.Attempts) != 0 {
		t.Fatalf("%+v %v", report, err)
	}
}

func TestConfigurationBoundaryValues(t *testing.T) {
	for _, tt := range []struct {
		name  string
		edit  func(*Config)
		valid bool
	}{
		{"minimums", func(c *Config) { c.APIKey, c.Model, c.Timeout, c.MaxAttempts = "!", "a", 100*time.Millisecond, 1 }, true},
		{"maximums", func(c *Config) {
			c.APIKey, c.Model, c.Timeout, c.MaxAttempts = strings.Repeat("~", 4096), strings.Repeat("m", 128), 2*time.Minute, 3
		}, true},
		{"allowed model punctuation", func(c *Config) { c.Model = "aZ09._:/-" }, true},
		{"key too long", func(c *Config) { c.APIKey = strings.Repeat("k", 4097) }, false},
		{"key DEL", func(c *Config) { c.APIKey = "key\x7f" }, false},
		{"key Unicode", func(c *Config) { c.APIKey = "key界" }, false},
		{"key tab", func(c *Config) { c.APIKey = "key\t" }, false},
		{"model too long", func(c *Config) { c.Model = strings.Repeat("m", 129) }, false},
		{"model Unicode", func(c *Config) { c.Model = "界" }, false},
		{"model whitespace", func(c *Config) { c.Model = "a b" }, false},
		{"timeout below minimum", func(c *Config) { c.Timeout = 100*time.Millisecond - 1 }, false},
		{"timeout above maximum", func(c *Config) { c.Timeout = 2*time.Minute + 1 }, false},
		{"zero attempts", func(c *Config) { c.MaxAttempts = 0 }, false},
		{"negative attempts", func(c *Config) { c.MaxAttempts = -1 }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config("https://trusted.example/responses")
			tt.edit(&cfg)
			if got := validConfig(cfg); got != tt.valid {
				t.Fatalf("validConfig=%v, want %v", got, tt.valid)
			}
		})
	}
	base := "https://trusted.example/"
	for _, tt := range []struct {
		endpoint string
		valid    bool
	}{
		{base + strings.Repeat("a", 2048-len(base)), true},
		{base + strings.Repeat("a", 2049-len(base)), false},
		{"https://trusted.example:1/path", true},
		{"https://trusted.example:65535/path", true},
		{"http://127.255.255.254/path", true},
		{"http://[::ffff:127.0.0.1]/path", true},
		{"http://[::]/path", false},
		{"//trusted.example/path", false},
		{"https:///path", false},
		{"https://trusted.example/path?token=secret", false},
		{"https://trusted.example/path#secret", false},
		{"https://trusted.example/path\x00", false},
		{"https://trusted.example:invalid/path", false},
	} {
		if got := validEndpoint(tt.endpoint); got != tt.valid {
			t.Errorf("endpoint valid=%v, want %v (length %d)", got, tt.valid, len(tt.endpoint))
		}
	}
}

func TestResponseShapesThroughHTTP(t *testing.T) {
	message := `{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"界🙂"}]}`
	for _, tt := range []struct {
		name, body, class string
		chars             int
	}{
		{"additive reasoning", `{"id":"r","object":"response","status":"completed","output":[{"type":"reasoning","summary":[{"future":true}]},` + message + `],"future":{"nested":true}}`, "success", 2},
		{"mixed refusal after text", `{"id":"r","object":"response","status":"completed","output":[` + message + `,{"type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"private"}]}]}`, "refusal", 0},
		{"missing output", `{"id":"r","object":"response","status":"completed"}`, "invalid_response", 0},
		{"unknown only", `{"id":"r","object":"response","status":"completed","output":[{"type":"reasoning"}]}`, "invalid_response", 0},
		{"empty", "", "invalid_response", 0},
		{"truncated", completed[:len(completed)-1], "invalid_response", 0},
		{"null output", strings.Replace(completed, `[{"type":"reasoning"},{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"O界"}]}]`, `null`, 1), "invalid_response", 0},
		{"object wrong type", strings.Replace(completed, `"object":"response"`, `"object":true`, 1), "invalid_response", 0},
		{"content wrong type", strings.Replace(completed, `[{"type":"output_text","text":"O界"}]`, `{}`, 1), "invalid_response", 0},
		{"text wrong type", strings.Replace(completed, `"text":"O界"`, `"text":7`, 1), "invalid_response", 0},
		{"null message", strings.Replace(completed, `"type":"message"`, `"type":null`, 1), "invalid_response", 0},
		{"missing object", strings.Replace(completed, `"object":"response",`, ``, 1), "invalid_response", 0},
		{"in progress", strings.Replace(completed, `"status":"completed"`, `"status":"in_progress"`, 1), "invalid_response", 0},
		{"queued", strings.Replace(completed, `"status":"completed"`, `"status":"queued"`, 1), "invalid_response", 0},
		{"canceled by provider", strings.Replace(completed, `"status":"completed"`, `"status":"canceled"`, 1), "invalid_response", 0},
		{"unfinished message", strings.Replace(completed, `"role":"assistant","status":"completed"`, `"role":"assistant","status":"in_progress"`, 1), "invalid_response", 0},
		{"null text", strings.Replace(completed, `"text":"O界"`, `"text":null`, 1), "invalid_response", 0},
		{"Unicode whitespace only", strings.Replace(completed, `"text":"O界"`, `"text":"\u2003\t\n"`, 1), "invalid_response", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, tt.body) }))
			defer server.Close()
			report, err := Run(context.Background(), config(server.URL))
			if report.Class != tt.class || report.OK != (tt.class == "success") || report.OutputChars != tt.chars || (err == nil) != report.OK || len(report.Attempts) != 1 {
				t.Fatalf("report=%+v error=%v", report, err)
			}
		})
	}
}

func TestRetryAttemptCapAndFallback(t *testing.T) {
	for _, max := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(429)
				io.WriteString(w, `{"error":{"type":"rate_limit_exceeded"}}`)
			}))
			defer server.Close()
			cfg := config(server.URL)
			cfg.MaxAttempts = max
			report, err := Run(context.Background(), cfg)
			if err == nil || report.Class != "rate_limit" || int(calls.Load()) != max || len(report.Attempts) != max {
				t.Fatalf("report=%+v calls=%d error=%v", report, calls.Load(), err)
			}
			for i, attempt := range report.Attempts {
				if attempt.Number != i+1 || attempt.HTTPStatus != 429 {
					t.Fatalf("invalid attempt sequence: %+v", report.Attempts)
				}
			}
		})
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.Header().Set("Retry-After", "invalid-private-header")
			w.WriteHeader(429)
			io.WriteString(w, `{"error":{"code":"rate_limit_exceeded"}}`)
			return
		}
		io.WriteString(w, completed)
	}))
	defer server.Close()
	cfg := config(server.URL)
	cfg.MaxAttempts, cfg.Timeout = 3, 3*time.Second
	report, err := Run(context.Background(), cfg)
	if err != nil || !report.OK || calls.Load() != 3 || len(report.Attempts) != 3 {
		t.Fatalf("fallback failed: %+v %v", report, err)
	}
	for i, bounds := range [][2]float64{{250, 375}, {500, 625}} {
		wait := report.Attempts[i].RetryWaitMS
		if wait < bounds[0] || wait > bounds[1] {
			t.Fatalf("attempt %d wait=%f, bounds=%v", i+1, wait, bounds)
		}
	}
	if report.TotalMS < report.Attempts[0].RetryWaitMS+report.Attempts[1].RetryWaitMS {
		t.Fatalf("total excludes retry waits: %+v", report)
	}
}

func TestCancellationInterruptsRetryWait(t *testing.T) {
	var calls atomic.Int32
	responded := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			defer close(responded)
		}
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		io.WriteString(w, `{"error":{"code":"rate_limit_exceeded"}}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-responded:
			time.Sleep(50 * time.Millisecond)
			cancel()
		case <-ctx.Done():
		}
	}()
	cfg := config(server.URL)
	cfg.Timeout = 3 * time.Second
	report, err := Run(ctx, cfg)
	if err == nil || report.Class != "canceled" || len(report.Attempts) != 1 || calls.Load() != 1 || report.Attempts[0].RetryWaitMS != 1000 {
		t.Fatalf("wait cancellation: report=%+v calls=%d error=%v", report, calls.Load(), err)
	}
}

func TestHeaderBudgetAndBodyBoundary(t *testing.T) {
	for _, tt := range []struct {
		name, class string
		header      string
		body        string
	}{
		{"header too large", "network", strings.Repeat("h", maxHeaderBytes+1), completed},
		{"body exactly limit", "success", "", completed + strings.Repeat(" ", maxBodyBytes-len(completed))},
		{"body over limit", "invalid_response", "", completed + strings.Repeat(" ", maxBodyBytes-len(completed)+1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("X-Untrusted", tt.header)
				io.WriteString(w, tt.body)
			}))
			defer server.Close()
			report, _ := Run(context.Background(), config(server.URL))
			if report.Class != tt.class || calls.Load() != 1 || len(report.Attempts) != 1 {
				t.Fatalf("report=%+v calls=%d", report, calls.Load())
			}
		})
	}
}

func TestTypedTransportFailuresAreSafeAndNeverRetried(t *testing.T) {
	for _, tt := range []struct {
		name, class string
		err         error
	}{
		{"dns", "dns", &net.DNSError{Err: "private-network-secret", Name: "private-host-secret"}},
		{"connect", "connect", &net.OpError{Op: "dial", Err: errors.New("private-network-secret")}},
		{"certificate", "tls", &tls.CertificateVerificationError{Err: errors.New("private-network-secret")}},
		{"unknown authority", "tls", x509.UnknownAuthorityError{}},
		{"hostname", "tls", x509.HostnameError{}},
		{"certificate invalid", "tls", x509.CertificateInvalidError{}},
		{"TLS record", "tls", tls.RecordHeaderError{Msg: "private-network-secret"}},
		{"generic", "network", errors.New("private-network-secret")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := http.DefaultTransport
			transport := original.(*http.Transport).Clone()
			var calls atomic.Int32
			transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, fmt.Errorf("wrapped: %w", tt.err)
			}
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
			report, err := Run(context.Background(), config("https://private-host-secret.invalid/private-url-secret"))
			var typed *Error
			if !errors.As(err, &typed) || typed.Class != tt.class || report.Class != tt.class || calls.Load() != 1 || len(report.Attempts) != 1 || report.Attempts[0].HTTPStatus != 0 {
				t.Fatalf("report=%+v calls=%d error=%v", report, calls.Load(), err)
			}
			encoded, marshalErr := json.Marshal(report)
			if marshalErr != nil || strings.Contains(string(encoded)+err.Error(), "private-") {
				t.Fatal("transport error leaked untrusted data")
			}
		})
	}
}

func TestProxyConfigurationIsIgnored(t *testing.T) {
	var proxyCalls atomic.Int32
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	transport.Proxy = func(*http.Request) (*url.URL, error) {
		proxyCalls.Add(1)
		return nil, errors.New("proxy must not be called")
	}
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, completed) }))
	defer server.Close()
	report, err := Run(context.Background(), config(server.URL))
	if err != nil || !report.OK || proxyCalls.Load() != 0 {
		t.Fatalf("proxy inherited: report=%+v calls=%d error=%v", report, proxyCalls.Load(), err)
	}
}

func TestRequestIDAllowlistAndSecretOmission(t *testing.T) {
	for _, tt := range []struct{ value, expected string }{
		{"req_123-A", "req_123-A"},
		{strings.Repeat("a", 128), strings.Repeat("a", 128)},
		{"", ""}, {strings.Repeat("a", 129), ""},
		{"prefix-fake-unique-key-suffix", ""},
		{"req.value", ""}, {"req value", ""}, {"req\x1b[31m", ""}, {"req\nforged", ""}, {"界", ""},
	} {
		if got := safeRequestID(tt.value, "fake-unique-key"); got != tt.expected {
			t.Fatalf("safeRequestID mismatch (length %d)", len(tt.value))
		}
	}
}
