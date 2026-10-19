package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daniel94104/llm-connect/internal/probe"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("llm-connect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	model := flags.String("model", "", "required Responses text model ID")
	endpoint := flags.String("endpoint", "https://api.openai.com/v1/responses", "trusted full Responses endpoint")
	timeout := flags.Duration("timeout", 20*time.Second, "whole-operation deadline (100ms to 2m)")
	attempts := flags.Int("max-attempts", 2, "maximum HTTP attempts (1 to 3)")
	jsonOutput := flags.Bool("json", false, "emit one safe JSON report")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			var help bytes.Buffer
			help.WriteString("Usage: llm-connect -model MODEL [options]\n")
			flags.SetOutput(&help)
			flags.PrintDefaults()
			if _, err := io.Copy(stdout, &help); err != nil {
				return 1
			}
			return 0
		}
		if _, err := fmt.Fprintln(stderr, "Invalid flags; use -h for usage."); err != nil {
			return 1
		}
		return 2
	}
	if flags.NArg() != 0 {
		if _, err := fmt.Fprintln(stderr, "Positional arguments are not supported; use -h for usage."); err != nil {
			return 1
		}
		return 2
	}
	report, err := probe.Run(ctx, probe.Config{
		Endpoint: *endpoint, Model: *model, APIKey: getenv("OPENAI_API_KEY"), Timeout: *timeout, MaxAttempts: *attempts,
	})
	if report.Class == "configuration" {
		if _, err := fmt.Fprintln(stderr, report.Message); err != nil {
			return 1
		}
		return 2
	}
	var writeErr error
	if *jsonOutput {
		writeErr = json.NewEncoder(stdout).Encode(report)
	} else {
		writeErr = writeText(stdout, report)
	}
	if writeErr != nil {
		return 1
	}
	if report.Class == "canceled" {
		return 130
	}
	if err != nil {
		return 1
	}
	return 0
}

func writeText(w io.Writer, report probe.Report) error {
	if _, err := fmt.Fprintf(w, "%s: %s\nTotal: %.2f ms; output: %d characters\n", report.Class, report.Message, report.TotalMS, report.OutputChars); err != nil {
		return err
	}
	for _, attempt := range report.Attempts {
		if _, err := fmt.Fprintf(w, "Attempt %d: HTTP %d; request_id=%s; timings_ms=%s; retry_wait_ms=%.2f\n", attempt.Number, attempt.HTTPStatus, attempt.RequestID, attempt.TimingsMS.String(), attempt.RetryWaitMS); err != nil {
			return err
		}
	}
	return nil
}
