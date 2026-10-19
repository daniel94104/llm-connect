# llm-connect

A small Go CLI that checks a Responses API endpoint by making a real, fixed text-generation request. It reports whether generation completed, safe failure classes, request IDs, and connection timings. Generated text and credentials are never printed.

This is an original integration sample using Go's standard library. Offline tests exercise local HTTP and TLS servers. They verify the client and protocol handling, not availability or access to any live account or model. No live provider call is part of development or CI.

## Build and test

Go 1.25 or newer is required. There are no third-party modules.

```sh
go build -o llm-connect .
go test -race ./...
go vet ./...
```

CI also checks formatting and builds both packages. Tests require permission to bind local loopback listeners and do not require API credentials.

## Optional live diagnostic

Use your own API account and explicitly choose a Responses text model available to your project. Running this command may incur API charges. The fixed request asks for a short reply with `max_output_tokens: 64`, `store: false`, and `stream: false`.

In Bash, read the key without placing it in shell history or a file:

```bash
read -r -s -p 'OPENAI_API_KEY: ' OPENAI_API_KEY
printf '\n'
export OPENAI_API_KEY
./llm-connect -model YOUR_RESPONSES_TEXT_MODEL -json
unset OPENAI_API_KEY
```

The token cap includes reasoning and visible output. Some reasoning models will return an incomplete response within this cap. Choose a suitable text model; the tool intentionally does not increase the cap automatically. Completion requires a valid completed assistant text message; the exact reply need not be `OK`.

Flags accept standard Go flag syntax, with one or two leading dashes:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-model` | Required | Explicit model ID, 1–128 permitted ASCII characters |
| `-endpoint` | `https://api.openai.com/v1/responses` | Trusted full Responses endpoint |
| `-timeout` | `20s` | Entire operation, including retries and body reads; 100ms–2m |
| `-max-attempts` | `2` | Total HTTP attempts, 1–3 |
| `-json` | `false` | One JSON report, including runtime failures |
| `-h` | | Help without credentials or network activity |

The only credential input is `OPENAI_API_KEY`. Positional arguments and credential flags are not supported. The tool validates configuration before sending a request.

## Reports and retries

Reports contain `ok`, `class`, `message`, `attempts`, `total_ms`, and `output_chars`. Each attempt has `number`, `http_status` (zero before a response), optional `request_id`, `timings_ms`, and optional `retry_wait_ms`. Timings use milliseconds. Unobserved DNS/connect/TLS/first-byte phases are omitted. Text output presents the same safe fields.

DNS, connect, and TLS values accumulate completed phase intervals. First byte is elapsed time from the attempt's start; attempt total includes reading the body. Overall total includes retry waits. Phases can overlap and are not components that sum to total. Requests use fresh HTTP/1.1 connections, so these measurements differ from applications using pooled connections or HTTP/2. A completed phase may have failed; timing alone does not indicate success.

Only HTTP 429 with an exact `rate_limit_exceeded` code or type is retried. `insufficient_quota` in either field always prevents retry. Unknown throttles, `slow_down`, malformed error bodies, 5xx, authentication failures, and transport failures are reported without automatic retry. This conservative policy avoids resending a generation request when its outcome is uncertain.

A valid `Retry-After` delay is a minimum. If it cannot fit within the remaining whole-operation deadline, no further request is sent. Missing or invalid hints use bounded exponential delay with jitter. The maximum attempt count includes the initial request. Ctrl-C or SIGTERM cancels work and retry waits.

Exit codes: `0` success; `2` invalid configuration or usage; `130` cancellation; `1` all other runtime or output failures. Configuration errors go to stderr with empty stdout, even with `-json`. Runtime JSON reports go to stdout with empty stderr. Output write failures return `1`.

Failure classes are `configuration`, `canceled`, `deadline`, `dns`, `connect`, `tls`, `network`, `auth`, `quota`, `rate_limit`, `provider`, `request`, `redirect`, `refusal`, `incomplete`, and `invalid_response`.

## Security and limits

Custom endpoints receive your API key and must be trusted. HTTPS uses normal certificate and hostname verification. Plain HTTP is permitted only for literal loopback IP addresses such as `127.0.0.1` and `[::1]`; `localhost` and non-loopback addresses are rejected. URLs with credentials, queries, fragments, or invalid ports are rejected. Proxy environment variables are deliberately ignored. Redirects are never followed.

Responses are limited to 1 MiB and response headers to 32 KiB. Response data and arbitrary upstream messages, URLs, model names, and headers are never printed. Request IDs are retained only when they contain 1–128 ASCII letters, digits, underscores, or hyphens and do not contain the key. Request bodies are at most 4 KiB. There are no credential files, debug dumps, persistent logs, cookies, telemetry exporters, or automatic transport retries.

`store: false` requests that the provider not store a response object; it does not eliminate all provider retention or gateway logging. Credentials remain in process memory until exit. Environment handling and retention policies of your terminal, operating system, and endpoint are outside this tool's control.

Protocol references: [Responses API](https://developers.openai.com/api/reference/resources/responses/methods/create), [text generation](https://developers.openai.com/api/docs/guides/text), and [rate limits](https://developers.openai.com/api/docs/guides/rate-limits).
