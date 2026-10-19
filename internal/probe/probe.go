// Package probe makes a bounded Responses request and returns safe diagnostic metadata.
package probe

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxBodyBytes    = 1 << 20
	maxRequestBytes = 4 << 10
	maxHeaderBytes  = 32 << 10
	maxOutputTokens = 64
	maxDuration     = 1<<63 - 1
)

// Config supplies an explicit trusted endpoint and an in-memory API credential.
type Config struct {
	Endpoint, Model, APIKey string
	Timeout                 time.Duration
	MaxAttempts             int
}

// Error is a classified failure containing only a fixed safe message.
type Error struct{ Class, Message string }

// Error returns a fixed safe diagnostic without an underlying private error.
func (e *Error) Error() string { return e.Message }

// Attempt describes one application-level POST, including failed transport attempts.
type Attempt struct {
	Number      int     `json:"number"`
	HTTPStatus  int     `json:"http_status"`
	RequestID   string  `json:"request_id,omitempty"`
	TimingsMS   Timings `json:"timings_ms"`
	RetryWaitMS float64 `json:"retry_wait_ms,omitempty"`
}

// Report contains only safe diagnostic metadata, never generated text or configuration.
type Report struct {
	OK          bool      `json:"ok"`
	Class       string    `json:"class"`
	Message     string    `json:"message"`
	Attempts    []Attempt `json:"attempts"`
	TotalMS     float64   `json:"total_ms"`
	OutputChars int       `json:"output_chars"`
}

var messages = map[string]string{
	"success":          "Response generation completed.",
	"configuration":    "Invalid configuration; set OPENAI_API_KEY, select -model, and check endpoint, timeout, and attempt limits with -h.",
	"canceled":         "Operation canceled; rerun when ready.",
	"deadline":         "Operation deadline exceeded; check connectivity or increase -timeout within its limit.",
	"dns":              "DNS resolution failed; check the endpoint hostname and resolver.",
	"connect":          "Connection failed; check endpoint availability and network access.",
	"tls":              "TLS verification or handshake failed; check the endpoint certificate and system trust.",
	"network":          "Network exchange failed; check connectivity before rerunning.",
	"auth":             "Authentication failed; check OPENAI_API_KEY and project/model access.",
	"quota":            "Quota is unavailable; check account billing and project limits.",
	"rate_limit":       "Rate limit reached; wait before rerunning and check project limits.",
	"provider":         "Provider failed; check provider status before rerunning.",
	"request":          "Request rejected; check endpoint and model compatibility.",
	"redirect":         "Endpoint redirected; choose a trusted final Responses endpoint explicitly.",
	"refusal":          "Generation was refused; check model and account policies.",
	"incomplete":       "Generation was incomplete; choose a text model suitable for the fixed 64-token limit.",
	"invalid_response": "Response was invalid or exceeded a size limit; check Responses API compatibility.",
}

func failed(report Report, class string) (Report, error) {
	report.Class, report.Message = class, messages[class]
	return report, &Error{Class: class, Message: report.Message}
}

func validConfig(c Config) bool {
	if c.Timeout < 100*time.Millisecond || c.Timeout > 2*time.Minute || c.MaxAttempts < 1 || c.MaxAttempts > 3 || len(c.APIKey) < 1 || len(c.APIKey) > 4096 || len(c.Model) < 1 || len(c.Model) > 128 {
		return false
	}
	for _, b := range []byte(c.APIKey) {
		if b < 33 || b > 126 {
			return false
		}
	}
	for _, b := range []byte(c.Model) {
		if !asciiAlnum(b) && !strings.ContainsRune("._:/-", rune(b)) {
			return false
		}
	}
	return validEndpoint(c.Endpoint)
}

func asciiAlnum(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

func validEndpoint(raw string) bool {
	if len(raw) == 0 || len(raw) > 2048 || strings.Contains(raw, "#") {
		return false
	}
	for _, b := range []byte(raw) {
		if b <= 32 || b == 127 {
			return false
		}
	}
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() || u.Opaque != "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(u.Hostname(), "%") || strings.HasSuffix(u.Host, ":") {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	if u.Scheme == "https" {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "http" && ip != nil && ip.IsLoopback()
}

// Run executes the diagnostic under one deadline, with retries only for recognized throttling.
func Run(parent context.Context, cfg Config) (report Report, err error) {
	start := time.Now()
	report.Attempts = []Attempt{}
	defer func() { report.TotalMS = milliseconds(time.Since(start)) }()
	if !validConfig(cfg) {
		return failed(report, "configuration")
	}
	ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()
	body, err := json.Marshal(struct {
		Model           string `json:"model"`
		Input           string `json:"input"`
		MaxOutputTokens int    `json:"max_output_tokens"`
		Store           bool   `json:"store"`
		Stream          bool   `json:"stream"`
	}{cfg.Model, "Reply with the single word OK.", maxOutputTokens, false, false})
	if err != nil || len(body) > maxRequestBytes {
		return failed(report, "configuration")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	transport.MaxResponseHeaderBytes = maxHeaderBytes
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for number := 1; number <= cfg.MaxAttempts; number++ {
		if ctx.Err() != nil {
			return failed(report, networkClass(ctx, ctx.Err()))
		}
		attempt, data, exchangeErr := exchange(ctx, client, cfg, body, number)
		report.Attempts = append(report.Attempts, attempt)
		if exchangeErr != nil {
			return failed(report, networkClass(ctx, exchangeErr))
		}
		if len(data.body) > maxBodyBytes {
			return failed(report, "invalid_response")
		}
		if attempt.HTTPStatus == http.StatusOK {
			class, count := validateResponse(data.body)
			report.OutputChars = count
			report.OK = class == "success"
			if !report.OK {
				return failed(report, class)
			}
			report.Class, report.Message = class, messages[class]
			return report, nil
		}
		class, retry := statusClass(attempt.HTTPStatus, data.body)
		report, err = failed(report, class)
		if !retry || number == cfg.MaxAttempts {
			return report, err
		}
		delay, valid := retryAfter(data.retryAfter, time.Now())
		if !valid {
			delay = 250*time.Millisecond*time.Duration(1<<(number-1)) + time.Duration(rand.Int64N(int64(125*time.Millisecond)+1))
		}
		deadline, _ := ctx.Deadline()
		if delay >= time.Until(deadline) {
			return report, err
		}
		report.Attempts[len(report.Attempts)-1].RetryWaitMS = milliseconds(delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return failed(report, networkClass(ctx, ctx.Err()))
		case <-timer.C:
		}
	}
	return report, err
}

type exchangeData struct {
	body       []byte
	retryAfter string
}

func exchange(ctx context.Context, client *http.Client, cfg Config, body []byte, number int) (attempt Attempt, data exchangeData, err error) {
	trace := &traceState{start: time.Now(), connect: make(map[string][]time.Time)}
	attempt.Number = number
	defer func() { attempt.TimingsMS = trace.snapshot() }()
	request, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace.callbacks()), http.MethodPost, cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return attempt, data, err
	}
	// A billable POST must never be transparently replayed by the transport.
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return attempt, data, err
	}
	defer response.Body.Close()
	attempt.HTTPStatus = response.StatusCode
	attempt.RequestID = safeRequestID(response.Header.Get("x-request-id"), cfg.APIKey)
	data.retryAfter = response.Header.Get("Retry-After")
	data.body, err = io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
	return attempt, data, err
}

func safeRequestID(value, key string) string {
	if len(value) < 1 || len(value) > 128 || strings.Contains(value, key) {
		return ""
	}
	for _, b := range []byte(value) {
		if !asciiAlnum(b) && b != '_' && b != '-' {
			return ""
		}
	}
	return value
}

func networkClass(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return "canceled"
	}
	var dns *net.DNSError
	var certificate *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var record tls.RecordHeaderError
	var operation *net.OpError
	if errors.As(err, &dns) {
		return "dns"
	}
	if errors.As(err, &certificate) || errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) || errors.As(err, &record) {
		return "tls"
	}
	if errors.As(err, &operation) && operation.Op == "dial" {
		return "connect"
	}
	return "network"
}

func decode(data []byte, value any) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(value) != nil {
		return false
	}
	return decoder.Decode(new(any)) == io.EOF
}

func statusClass(status int, body []byte) (string, bool) {
	if status == 401 || status == 403 {
		return "auth", false
	}
	if status >= 500 && status < 600 {
		return "provider", false
	}
	if status >= 300 && status < 400 {
		return "redirect", false
	}
	if status != 429 {
		return "request", false
	}
	var envelope struct {
		Error *struct {
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
	}
	if !decode(body, &envelope) || envelope.Error == nil {
		return "rate_limit", false
	}
	code, kind := envelope.Error.Code, envelope.Error.Type
	if code == "insufficient_quota" || kind == "insufficient_quota" {
		return "quota", false
	}
	if code == "slow_down" || kind == "slow_down" {
		return "rate_limit", false
	}
	return "rate_limit", code == "rate_limit_exceeded" || kind == "rate_limit_exceeded"
}

func validateResponse(body []byte) (string, int) {
	var response struct {
		ID     string          `json:"id"`
		Object string          `json:"object"`
		Status string          `json:"status"`
		Error  json.RawMessage `json:"error"`
		Output []struct {
			Type    string `json:"type"`
			Role    string `json:"role"`
			Status  string `json:"status"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if !decode(body, &response) || response.Object != "response" || response.ID == "" || len(response.Error) > 0 && string(response.Error) != "null" {
		return "invalid_response", 0
	}
	count := 0
	for _, item := range response.Output {
		if item.Type != "message" {
			continue
		}
		for _, content := range item.Content {
			if content.Type == "refusal" {
				return "refusal", 0
			}
			if item.Role == "assistant" && item.Status == "completed" && content.Type == "output_text" && strings.TrimSpace(content.Text) != "" {
				count += utf8.RuneCountInString(content.Text)
			}
		}
	}
	if response.Status == "incomplete" {
		return "incomplete", 0
	}
	if response.Status != "completed" || count == 0 {
		return "invalid_response", 0
	}
	return "success", count
}

func retryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	digits := true
	for _, b := range []byte(value) {
		if b < '0' || b > '9' {
			digits = false
			break
		}
	}
	if digits {
		seconds, err := strconv.ParseUint(value, 10, 64)
		// Syntactically valid huge minima cannot fall back to a shorter retry.
		if err != nil || seconds > uint64(maxDuration/int64(time.Second)) {
			return time.Duration(maxDuration), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	date, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := date.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}
