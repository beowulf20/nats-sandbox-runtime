package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/draw"
	"image/png"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"nats-sandbox-runtime/pkg/pyruntime"
)

//go:embed guest.py
var guestCode string

func status() map[string]int64 {
	b, _ := os.ReadFile("/proc/self/status")
	out := map[string]int64{}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && (f[0] == "VmRSS:" || f[0] == "VmHWM:" || f[0] == "VmSwap:") {
			v, _ := strconv.ParseInt(f[1], 10, 64)
			out[strings.TrimSuffix(f[0], ":")+"_kib"] = v
		}
	}
	return out
}

func main() {
	url := flag.String("url", "", "NATS URL")
	bucket := flag.String("bucket", "", "bucket")
	parallel := flag.Int("parallel", 2, "requests per batch")
	repeats := flag.Int("repeats", 5, "batches")
	phase := flag.String("phase", "measured", "phase")
	workload := flag.String("workload", "small", "small, io, plot")
	_ = flag.Int("size-mib", 1, "compatibility")
	_ = flag.Int("iterations", 1, "compatibility")
	_ = flag.Int("files", 0, "compatibility")
	profile := flag.String("import-profile", "", "empty, numpy, plot-agg")
	charts := flag.Int("charts", 1, "charts per plot request")
	compression := flag.Int("png-compression", 6, "PNG compression 0 through 9")
	flag.Parse()
	if *charts < 1 || *compression < 0 || *compression > 9 {
		panic("invalid plot parameters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	c, err := pyruntime.New(ctx, pyruntime.Config{URL: *url, Bucket: *bucket})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	payloadHash := sha256.New()
	chunk := bytes.Repeat([]byte("x"), 65536)
	for range 128 {
		payloadHash.Write(chunk)
	}
	expectedPayloadHash := hex.EncodeToString(payloadHash.Sum(nil))
	encoder := json.NewEncoder(os.Stdout)
	for rep := range *repeats {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		results := make([]map[string]any, *parallel)
		var wg sync.WaitGroup
		for lane := range *parallel {
			wg.Go(func() {
				runStart := time.Now()
				r := map[string]any{"lane": lane}
				results[lane] = r
				mode := *workload
				if *phase == "warmup" {
					mode = "small"
				}
				files := map[string][]byte{}
				inputBytes := int64(0)
				if *phase != "warmup" {
					count, size := 65, 1024
					if mode == "io" {
						count, size = 4, 4*1024*1024
					}
					for i := range count {
						payload := bytes.Repeat([]byte{byte(i + lane + rep + 1)}, size)
						files[fmt.Sprintf("artifacts/%03d.bin", i)] = payload
						inputBytes += int64(size)
					}
				}
				thread := fmt.Sprintf("lane-%d", lane)
				code := fmt.Sprintf("thread=%q\nmode=%q\nrun_id=%d\nprofile=%q\ncharts=%d\ncompression=%d\n", thread, mode, rep, *profile, *charts, *compression) + guestCode
				result, err := c.Run(ctx, pyruntime.Request{ImportProfile: *profile, ThreadID: thread, Code: code, Files: files, ArtifactExcludePrefixes: []string{"artifacts/"}})
				r["elapsed_ms"] = float64(time.Since(runStart)) / float64(time.Millisecond)
				if err != nil {
					r["error"] = err.Error()
					return
				}
				var metrics map[string]any
				if err := json.Unmarshal(result.Files["outputs/metrics.json"], &metrics); err != nil {
					r["error"] = fmt.Sprint("metrics: ", err)
					return
				}
				if metrics["optimize"] != float64(0) || metrics["debug"] != true {
					r["error"] = "Python assertions disabled"
					return
				}
				if mode == "backend" && !strings.EqualFold(fmt.Sprint(metrics["backend"]), "svg") {
					r["error"] = fmt.Sprintf("caller backend expected svg; got %v", metrics["backend"])
					return
				}
				hashes := metrics["input_hashes"].(map[string]any)
				for name, b := range files {
					sum := sha256.Sum256(b)
					if hashes[strings.TrimPrefix(name, "artifacts/")] != hex.EncodeToString(sum[:]) {
						r["error"] = "input checksum mismatch"
						return
					}
				}
				if string(result.Files["thread-marker"]) != thread {
					r["error"] = "thread isolation failure"
					return
				}
				outputBytes := int64(0)
				for path, b := range result.Files {
					if strings.HasPrefix(path, "artifacts/") {
						r["error"] = "excluded input returned"
						return
					}
					outputBytes += int64(len(b))
				}
				if mode == "io" {
					sum := sha256.Sum256(result.Files["outputs/payload.bin"])
					if hex.EncodeToString(sum[:]) != expectedPayloadHash {
						r["error"] = "output checksum mismatch"
						return
					}
				}
				if mode == "plot" {
					pixelHashes := []string{}
					for chart := range *charts {
						name := fmt.Sprintf("outputs/chart-%d.png", chart)
						img, err := png.Decode(bytes.NewReader(result.Files[name]))
						if err != nil {
							r["error"] = fmt.Sprint("invalid plot: ", err)
							return
						}
						bounds := img.Bounds()
						rgba := image.NewRGBA(bounds)
						draw.Draw(rgba, bounds, img, bounds.Min, draw.Src)
						sum := sha256.Sum256(rgba.Pix)
						pixelHashes = append(pixelHashes, hex.EncodeToString(sum[:]))
					}
					r["pixel_hashes"] = pixelHashes
				}
				r["input_bytes"], r["output_bytes"], r["artifact_count"], r["guest"] = inputBytes, outputBytes, len(result.Files), metrics
				r["throughput_mib_s"] = float64(inputBytes+outputBytes) / (1024 * 1024) / (time.Since(runStart).Seconds())
			})
		}
		wg.Wait()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		encoder.Encode(map[string]any{"phase": *phase, "workload": *workload, "repetition": rep, "parallel": *parallel, "batch_ms": float64(time.Since(start)) / float64(time.Millisecond), "client": status(), "client_allocated_bytes": after.TotalAlloc - before.TotalAlloc, "results": results})
		for _, r := range results {
			if r["error"] != nil {
				os.Exit(1)
			}
		}
	}
}
