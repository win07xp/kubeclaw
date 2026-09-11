//go:build loadtest

/*
Copyright 2026 The Kaalm Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The in-cluster load generator. It runs as a Job in the fleet's namespace,
// authenticates the way a workload would (a projected ServiceAccount token for
// the LLM listener, the channel's bearer secret for the user listener), and
// prints one RESULT_JSON line the orchestrator parses from the pod log.

const resultMarker = "RESULT_JSON "

type loadResult struct {
	Mode        string         `json:"mode"`
	Requests    int            `json:"requests"`
	Statuses    map[string]int `json:"statuses"`
	DurationSec float64        `json:"durationSec"`
	RPS         float64        `json:"rps"`
	LatencyMs   stats          `json:"latencyMs"`
	Errors      []string       `json:"errors,omitempty"`
	WarmupSec   float64        `json:"warmupSec"`
}

type loadgenConfig struct {
	mode        string
	url         string
	model       string
	concurrency int
	duration    time.Duration
	tokenFile   string
	caFile      string
	bearerFile  string
	base        string
	pathPrefix  string
	count       int
	pad         int
	rate        float64
	warmup      time.Duration
	noKeepalive bool
	certFile    string
	keyFile     string
}

func runLoadgen(args []string) error {
	var c loadgenConfig
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.StringVar(&c.mode, "mode", modeGateway,
		"gateway (LLM proxy at fixed concurrency) or channels (webhook messages at a fixed rate)")
	fs.StringVar(&c.url, "url", "https://kaalm-gateway.kaalm-system.svc:8443/v1/chat/completions",
		"gateway mode: LLM endpoint")
	fs.StringVar(&c.model, "model", providerFast+"/mock-model", "gateway mode: qualified provider/model")
	fs.IntVar(&c.concurrency, "concurrency", 32, "gateway mode: concurrent callers")
	fs.DurationVar(&c.duration, "duration", time.Minute, "measured run length after warmup")
	fs.StringVar(&c.tokenFile, "token-file", "/var/run/token/token",
		"projected ServiceAccount token (audience kaalm-gateway)")
	fs.StringVar(&c.caFile, "ca-file", "/var/run/ca/ca.crt", "the kaalm-ca bundle the gateway's certificates chain to")
	fs.StringVar(&c.bearerFile, "bearer-file", "/var/run/hook/token", "channels mode: the webhook bearer secret")
	fs.StringVar(&c.base, "base", "https://kaalm-gateway.kaalm-system.svc:8080", "channels mode: user listener base URL")
	fs.StringVar(&c.pathPrefix, "path-prefix", "/channels/load/ramp-",
		"channels mode: channel path prefix; the index is appended")
	fs.IntVar(&c.count, "count", 1, "channels mode: number of channels under the prefix")
	fs.IntVar(&c.pad, "pad", 4, "channels mode: zero-padding width of the index")
	fs.Float64Var(&c.rate, "rate", 1, "channels mode: messages per second across all channels, round-robin")
	fs.DurationVar(&c.warmup, "warmup", 90*time.Second, "how long to retry until the first success before measuring")
	fs.BoolVar(&c.noKeepalive, "no-keepalive", false,
		"open a fresh connection per request (measures the cost of an in-cluster dial: DNS, TCP, TLS)")
	fs.StringVar(&c.certFile, "cert-file", "",
		"gateway mode: present this client certificate (a Kaalm agent identity) instead of the ServiceAccount token")
	fs.StringVar(&c.keyFile, "key-file", "", "gateway mode: the client certificate's key")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ca, err := os.ReadFile(c.caFile)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return fmt.Errorf("no certificates in %s", c.caFile)
	}
	tlsCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if c.certFile != "" {
		cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
		if err != nil {
			return fmt.Errorf("client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   c.noKeepalive,
	}
	cli := &http.Client{Transport: transport, Timeout: 60 * time.Second}

	var res *loadResult
	switch c.mode {
	case modeGateway:
		res, err = gatewayLoad(cli, c)
	case modeChannels:
		res, err = channelLoad(cli, c)
	default:
		return fmt.Errorf("unknown mode %q", c.mode)
	}
	if err != nil {
		return err
	}
	out, _ := json.Marshal(res)
	fmt.Println(resultMarker + string(out))
	return nil
}

// recorder collects per-request outcomes from concurrent workers.
type recorder struct {
	mu        sync.Mutex
	latencies []float64
	statuses  map[string]int
	errors    []string
}

func newRecorder() *recorder { return &recorder{statuses: map[string]int{}} }

func (r *recorder) record(status int, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.statuses["error"]++
		if len(r.errors) < 5 {
			r.errors = append(r.errors, err.Error())
		}
		return
	}
	r.statuses[strconv.Itoa(status)]++
	r.latencies = append(r.latencies, float64(latency.Microseconds())/1000)
}

func (r *recorder) result(mode string, elapsed, warmup time.Duration) *loadResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, n := range r.statuses {
		total += n
	}
	return &loadResult{
		Mode:        mode,
		Requests:    total,
		Statuses:    r.statuses,
		DurationSec: round3(elapsed.Seconds()),
		RPS:         round3(float64(total) / elapsed.Seconds()),
		LatencyMs:   summarize(r.latencies),
		Errors:      r.errors,
		WarmupSec:   round3(warmup.Seconds()),
	}
}

func doRequest(cli *http.Client, req *http.Request) (int, time.Duration, error) {
	start := time.Now()
	resp, err := cli.Do(req)
	if err != nil {
		return 0, time.Since(start), err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
	return resp.StatusCode, time.Since(start), nil
}

// gatewayLoad drives chat completions at fixed concurrency: every worker
// issues the next request as soon as the previous one returns.
func gatewayLoad(cli *http.Client, c loadgenConfig) (*loadResult, error) {
	// An mTLS caller is identified by its certificate; only the token tier
	// sends a bearer.
	bearer := ""
	if c.certFile == "" {
		token, err := os.ReadFile(c.tokenFile)
		if err != nil {
			return nil, err
		}
		bearer = strings.TrimSpace(string(token))
	}
	body := []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"load"}]}`, c.model))
	newReq := func() *http.Request {
		req, _ := http.NewRequest(http.MethodPost, c.url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		return req
	}

	// Warmup: the gateway's source-IP cross-check answers 401 until its Pod
	// informer has seen this freshly created caller; a real client retries
	// the same way. Nothing is measured until the first 200.
	warmStart := time.Now()
	for {
		status, _, err := doRequest(cli, newReq())
		if err == nil && status == http.StatusOK {
			break
		}
		if time.Since(warmStart) > c.warmup {
			return nil, fmt.Errorf("warmup: no 200 within %s (last status %d, err %v)", c.warmup, status, err)
		}
		time.Sleep(2 * time.Second)
	}
	warmup := time.Since(warmStart)

	rec := newRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), c.duration)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				status, lat, err := doRequest(cli, newReq())
				rec.record(status, lat, err)
			}
		}()
	}
	wg.Wait()
	return rec.result(modeGateway, time.Since(start), warmup), nil
}

// channelLoad posts webhook messages round-robin across the channels at a
// fixed aggregate rate, so message i lands on channel i mod count. With rate =
// count/interval every channel receives exactly one message per interval,
// which is how the hold phase keeps the whole fleet lightly served and the
// churn phase makes nearly every message a cold wake.
func channelLoad(cli *http.Client, c loadgenConfig) (*loadResult, error) {
	bearerRaw, err := os.ReadFile(c.bearerFile)
	if err != nil {
		return nil, err
	}
	bearer := strings.TrimSpace(string(bearerRaw))
	if c.count <= 0 || c.rate <= 0 {
		return nil, fmt.Errorf("channels mode needs count > 0 and rate > 0")
	}
	url := func(i int) string {
		return fmt.Sprintf("%s%s%0*d", c.base, c.pathPrefix, c.pad, i)
	}
	newReq := func(i int) *http.Request {
		body := fmt.Sprintf(`{"userId":"load","content":{"text":"ping %d"}}`, i)
		req, _ := http.NewRequest(http.MethodPost, url(i), strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		return req
	}

	// Warmup against channel 0 until the gateway accepts (202 for async).
	warmStart := time.Now()
	for {
		status, _, err := doRequest(cli, newReq(0))
		if err == nil && (status == http.StatusAccepted || status == http.StatusOK) {
			break
		}
		if time.Since(warmStart) > c.warmup {
			return nil, fmt.Errorf("warmup: channel 0 never accepted within %s (last status %d, err %v)", c.warmup, status, err)
		}
		time.Sleep(2 * time.Second)
	}
	warmup := time.Since(warmStart)

	rec := newRecorder()
	interval := time.Duration(float64(time.Second) / c.rate)
	workers := int(c.rate*3) + 4
	if workers > 128 {
		workers = 128
	}
	jobs := make(chan int, workers*2)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				status, lat, err := doRequest(cli, newReq(i))
				rec.record(status, lat, err)
			}
		}()
	}
	start := time.Now()
	ticker := time.NewTicker(interval)
	next := 0
	for time.Since(start) < c.duration {
		<-ticker.C
		select {
		case jobs <- next % c.count:
		default:
			rec.record(0, 0, fmt.Errorf("workers saturated at %.1f msg/s", c.rate))
		}
		next++
	}
	ticker.Stop()
	close(jobs)
	wg.Wait()
	return rec.result(modeChannels, time.Since(start), warmup), nil
}
