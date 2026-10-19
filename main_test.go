package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daniel94104/llm-connect/internal/probe"
)

func fakeEnv(name string) string {
	if name == "OPENAI_API_KEY" {
		return "fake-cli-secret"
	}
	return ""
}

func TestHelpAndConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		key  string
		code int
	}{
		{"help", []string{"-h"}, "", 0},
		{"missing model", nil, "fake", 2},
		{"missing key", []string{"-model", "chosen-model"}, "", 2},
		{"bad key", []string{"-model", "chosen-model"}, "private\nsecret", 2},
		{"bad flag", []string{"-private-secret"}, "", 2},
		{"bad duration", []string{"-timeout", "private-secret"}, "", 2},
		{"positional", []string{"private-secret"}, "", 2},
		{"bad endpoint", []string{"-json", "-model", "chosen-model", "-endpoint", "http://private-secret.invalid/?private-secret"}, "fake", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			code := run(context.Background(), tt.args, func(string) string { return tt.key }, &out, &stderr)
			if code != tt.code {
				t.Fatalf("code=%d out=%q stderr=%q", code, out.String(), stderr.String())
			}
			if tt.code == 2 && out.Len() != 0 {
				t.Fatalf("configuration wrote stdout")
			}
			if strings.Contains(out.String()+stderr.String(), "private-secret") || strings.Contains(out.String()+stderr.String(), "private\nsecret") {
				t.Fatalf("private input leaked")
			}
		})
	}
}

func TestRuntimeOutput(t *testing.T) {
	for _, tt := range []struct {
		name, body, class string
		status, code      int
	}{
		{"success", `{"id":"resp_test","object":"response","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"private-generated-text"}]}]}`, "success", 200, 0},
		{"auth", `{"error":{"message":"private-generated-text fake-cli-secret"}}`, "auth", 401, 1},
	} {
		for _, jsonFlag := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{true: " JSON", false: " text"}[jsonFlag], func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("x-request-id", "fake-cli-secret")
					w.WriteHeader(tt.status)
					io.WriteString(w, tt.body)
				}))
				defer server.Close()
				args := []string{"-model", "chosen-model", "-endpoint", server.URL}
				if jsonFlag {
					args = append(args, "-json")
				}
				var out, stderr bytes.Buffer
				if code := run(context.Background(), args, fakeEnv, &out, &stderr); code != tt.code {
					t.Fatalf("code=%d out=%q", code, out.String())
				}
				if stderr.Len() != 0 {
					t.Fatalf("runtime stderr=%q", stderr.String())
				}
				if strings.Contains(out.String(), "fake-cli-secret") || strings.Contains(out.String(), "private-generated-text") || strings.Contains(out.String(), server.URL) {
					t.Fatalf("private value leaked")
				}
				if jsonFlag {
					var report struct {
						Class string `json:"class"`
					}
					if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Class != tt.class {
						t.Fatalf("invalid JSON: %q %v", out.String(), err)
					}
				} else if !strings.Contains(out.String(), "timings_ms=") {
					t.Fatalf("missing timings: %q", out.String())
				}
			})
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("private-writer-error") }

func TestCancellationAndWriterFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := []string{"-model", "chosen-model", "-endpoint", "http://127.0.0.1:1", "-json"}
	var out, stderr bytes.Buffer
	if code := run(ctx, args, fakeEnv, &out, &stderr); code != 130 || !strings.Contains(out.String(), `"class":"canceled"`) {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
	if code := run(ctx, args, fakeEnv, failingWriter{}, &stderr); code != 1 {
		t.Fatalf("writer failure code=%d", code)
	}
	if code := run(ctx, []string{"-h"}, fakeEnv, failingWriter{}, &stderr); code != 1 {
		t.Fatalf("help writer failure code=%d", code)
	}
	if code := run(ctx, nil, fakeEnv, &out, failingWriter{}); code != 1 {
		t.Fatalf("configuration writer failure code=%d", code)
	}
	if code := run(ctx, []string{"-unknown"}, fakeEnv, &out, failingWriter{}); code != 1 {
		t.Fatalf("usage writer failure code=%d", code)
	}
	if strings.Contains(stderr.String(), "private-writer-error") {
		t.Fatal("writer error leaked")
	}
}

func TestHelpAndInvalidFlagsDoNotReadCredentials(t *testing.T) {
	for _, tt := range []struct {
		args []string
		code int
	}{
		{[]string{"-h"}, 0},
		{[]string{"-timeout", "private-flag-value"}, 2},
		{[]string{"-max-attempts", "private-flag-value"}, 2},
		{[]string{"-json=private-flag-value"}, 2},
		{[]string{"private-positional-value"}, 2},
	} {
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), tt.args, func(string) string {
			t.Fatal("credentials read before help/usage completed")
			return ""
		}, &stdout, &stderr)
		if code != tt.code || strings.Contains(stdout.String()+stderr.String(), "private-") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
		if tt.code == 0 && (stderr.Len() != 0 || !strings.Contains(stdout.String(), "Usage:")) {
			t.Fatalf("help streams: stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
		if tt.code == 2 && (stdout.Len() != 0 || stderr.Len() == 0) {
			t.Fatalf("usage streams: stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
	}
}

func TestDeadlineRuntimeProducesOneJSONReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-json", "-model", "chosen-model", "-endpoint", server.URL, "-timeout", "100ms"}, fakeEnv, &stdout, &stderr)
	var report probe.Report
	decoder := json.NewDecoder(&stdout)
	if err := decoder.Decode(&report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if code != 1 || stderr.Len() != 0 || report.Class != "deadline" || report.OK || len(report.Attempts) != 1 || report.Attempts[0].HTTPStatus != 200 {
		t.Fatalf("code=%d report=%+v stderr=%q", code, report, stderr.String())
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatalf("extra output after report: %v", err)
	}
}

type failAfterWriter struct{ remaining int }

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.remaining < len(p) {
		return 0, errors.New("private-writer-error")
	}
	w.remaining -= len(p)
	return len(p), nil
}

func TestTextWriterFailureOnAttemptLineIsReturned(t *testing.T) {
	report := probe.Report{Class: "success", Message: "Completed", Attempts: []probe.Attempt{{Number: 1, HTTPStatus: 200}}}
	var firstLine bytes.Buffer
	withoutAttempts := report
	withoutAttempts.Attempts = nil
	if err := writeText(&firstLine, withoutAttempts); err != nil {
		t.Fatal(err)
	}
	if err := writeText(&failAfterWriter{remaining: firstLine.Len()}, report); err == nil {
		t.Fatal("attempt-line write failure was discarded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stderr bytes.Buffer
	if code := run(ctx, []string{"-model", "chosen-model"}, fakeEnv, failingWriter{}, &stderr); code != 1 || stderr.Len() != 0 {
		t.Fatalf("text writer failure code=%d stderr=%q", code, stderr.String())
	}
	if code := run(ctx, []string{"unexpected"}, fakeEnv, io.Discard, failingWriter{}); code != 1 {
		t.Fatalf("positional error writer failure code=%d", code)
	}
}
