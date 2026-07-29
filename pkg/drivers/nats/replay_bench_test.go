package nats

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

// The cold-start replay benchmark models the stream shape reported in
// k3s-io/kine#720: a bucket whose sequence range is dominated by interior
// deletes (evicted by MaxMsgsPerSubject) with a comparatively small live set.
// The reported production stream was 77,361 subjects / 307,279 live messages /
// 1.3 GiB spread over a 25.4M sequence range. The defaults below preserve the
// live-to-deleted ratio at a scale that completes on a laptop; override via env
// to approach production size.
//
//	KINE_BENCH_KEYS     unique keys           (default 5000)
//	KINE_BENCH_REVS     writes per key        (default 24)
//	KINE_BENCH_VALSIZE  value bytes per write (default 2048)
//	KINE_BENCH_HISTORY  bucket History        (default 10)
//
// With the defaults: 120k writes, 50k live messages, 70k interior deletes.

type benchParams struct {
	keys    int
	revs    int
	valSize int
	history uint8
}

func loadBenchParams() benchParams {
	envInt := func(name string, def int) int {
		if v := os.Getenv(name); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
		return def
	}
	return benchParams{
		keys:    envInt("KINE_BENCH_KEYS", 5000),
		revs:    envInt("KINE_BENCH_REVS", 24),
		valSize: envInt("KINE_BENCH_VALSIZE", 2048),
		history: uint8(envInt("KINE_BENCH_HISTORY", 10)),
	}
}

// benchRun namespaces the keys each sub-benchmark writes. Go re-invokes a
// benchmark body for -count repeats and while sizing b.N, so deriving key names
// from b.N made a rerun collide with its own earlier keys and fail on ErrKeyExists.
var benchRun atomic.Int64

func (p benchParams) String() string {
	return fmt.Sprintf("keys=%d revs=%d valSize=%d history=%d", p.keys, p.revs, p.valSize, p.history)
}

// benchKey mimics the shape of a Kubernetes registry key, which determines the
// number of base58-encoded subject tokens per message.
func benchKey(i int) string {
	return fmt.Sprintf("/registry/pods/namespace-%d/pod-%08d", i%64, i)
}

// benchValue produces partially compressible bytes, closer to serialized
// Kubernetes objects than to random noise.
func benchValue(rnd *rand.Rand, size int) []byte {
	buf := make([]byte, size)
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-./: {}\"" // repetitive enough for s2 to bite
	for i := range buf {
		buf[i] = alphabet[rnd.Intn(len(alphabet))]
	}
	// Repeat a chunk so s2 finds matches, as it does in real object payloads.
	if size > 256 {
		copy(buf[size/2:], buf[:size/2])
	}
	return buf
}

func startBenchServer(tb testing.TB, storeDir string) *server.Server {
	tb.Helper()
	ns := test.RunServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  storeDir,
	})
	if !ns.ReadyForConnections(20 * time.Second) {
		tb.Fatal("nats server not ready")
	}
	return ns
}

// populateBucket writes the synthetic history directly to the KV subjects using
// async publishes. It intentionally bypasses Backend.Create/Update: those are
// synchronous round trips and would dominate setup time, and the resulting
// stream is identical in shape to one written by the backend.
func populateBucket(ctx context.Context, tb testing.TB, js jetstream.JetStream, bucket string, p benchParams) (live int, wire int64) {
	tb.Helper()

	kc := &keyCodec{}
	vc := &valueCodec{}
	rnd := rand.New(rand.NewSource(1))

	subject := func(key string) string {
		ek, err := kc.Encode(key)
		if err != nil {
			tb.Fatalf("encode key %q: %v", key, err)
		}
		return fmt.Sprintf("$KV.%s.%s", bucket, ek)
	}

	encode := func(key string, value []byte, create, del bool, prevRev int64) []byte {
		nd := natsData{
			Create:       create,
			Delete:       del,
			PrevRevision: prevRev,
			KV: &kserver.KeyValue{
				Key:   key,
				Value: value,
				Lease: 0,
			},
		}
		raw, err := nd.Encode()
		if err != nil {
			tb.Fatalf("encode natsData: %v", err)
		}
		buf := new(bytes.Buffer)
		if err := vc.Encode(raw, buf); err != nil {
			tb.Fatalf("compress natsData: %v", err)
		}
		return buf.Bytes()
	}

	var pending int
	flush := func() {
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(2 * time.Minute):
			tb.Fatal("timeout flushing async publishes")
		}
		pending = 0
	}

	publish := func(subj string, data []byte, hdr nats.Header) {
		msg := &nats.Msg{Subject: subj, Data: data, Header: hdr}
		if _, err := js.PublishMsgAsync(msg); err != nil {
			tb.Fatalf("publish: %v", err)
		}
		wire += int64(len(data))
		pending++
		if pending >= 4000 {
			flush()
		}
	}

	for i := 0; i < p.keys; i++ {
		key := benchKey(i)
		subj := subject(key)
		// Every 20th key ends deleted, exercising the tombstone-PUT + DEL-marker pair.
		deleted := i%20 == 0

		for r := 0; r < p.revs; r++ {
			publish(subj, encode(key, benchValue(rnd, p.valSize), r == 0, false, int64(r)), nil)
		}
		if deleted {
			publish(subj, encode(key, benchValue(rnd, p.valSize), false, true, int64(p.revs)), nil)
			publish(subj, nil, nats.Header{"KV-Operation": []string{"DEL"}})
		}
	}
	flush()

	// Live message count after MaxMsgsPerSubject eviction.
	str, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		tb.Fatalf("stream info: %v", err)
	}
	info, err := str.Info(ctx)
	if err != nil {
		tb.Fatalf("stream info: %v", err)
	}
	tb.Logf("populated stream: msgs=%d bytes=%d firstSeq=%d lastSeq=%d deleted=%d subjects=%d",
		info.State.Msgs, info.State.Bytes, info.State.FirstSeq, info.State.LastSeq,
		info.State.NumDeleted, info.State.NumSubjects)

	return int(info.State.Msgs), int64(info.State.Bytes)
}

// setupReplayFixture builds (once) a populated JetStream store on disk and
// returns the server plus a description of the stream. The store directory is
// reused across benchmark iterations so each iteration measures a genuine cold
// start against identical data.
func setupReplayFixture(tb testing.TB, p benchParams) (*server.Server, string) {
	tb.Helper()

	storeDir, err := os.MkdirTemp("", "kine-replay-bench-")
	if err != nil {
		tb.Fatalf("temp dir: %v", err)
	}
	tb.Cleanup(func() { os.RemoveAll(storeDir) })

	ns := startBenchServer(tb, storeDir)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "kine",
		History: p.history,
	}); err != nil {
		tb.Fatalf("create bucket: %v", err)
	}
	if err := ensureDirectGets(ctx, js, &Config{bucket: "kine"}); err != nil {
		tb.Fatalf("enable direct gets: %v", err)
	}

	start := time.Now()
	populateBucket(ctx, tb, js, "kine", p)
	tb.Logf("population took %s (%s)", time.Since(start).Round(time.Millisecond), p)

	return ns, storeDir
}

// measureReplay performs one cold start of the index against an already
// populated bucket and returns how long it took to become ready, along with the
// bytes the client pulled off the wire to get there.
func measureReplay(tb testing.TB, ns *server.Server, p benchParams) (dur time.Duration, inBytes uint64, inMsgs uint64, kv *KeyValue, cleanup func()) {
	tb.Helper()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	bkt, err := js.KeyValue(ctx, "kine")
	if err != nil {
		cancel()
		tb.Fatalf("bind bucket: %v", err)
	}

	l := logrus.New()
	l.SetOutput(io.Discard)
	b := Backend{l: l}

	wg := &sync.WaitGroup{}
	kv = NewKeyValue("bench", wg, bkt, js, int(p.history), b.Delete)
	b.kv = kv

	before := nc.Stats()

	// Profiling is scoped to the replay window only, so the profile is not
	// polluted by fixture population (which does the same s2/JSON work).
	if path := os.Getenv("KINE_BENCH_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			tb.Fatalf("create cpu profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			tb.Fatalf("start cpu profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	start := time.Now()
	kv.Start(ctx)

	// Wait on readyCh directly rather than through waitReady, so the benchmark
	// is not capped by the hardcoded readiness timeout it exists to measure.
	select {
	case <-kv.readyCh:
	case <-time.After(15 * time.Minute):
		cancel()
		wg.Wait()
		nc.Close()
		tb.Fatal("replay did not complete within 15m")
	}
	dur = time.Since(start)

	after := nc.Stats()

	cleanup = func() {
		cancel()
		wg.Wait()
		nc.Close()
	}

	return dur, after.InBytes - before.InBytes, after.InMsgs - before.InMsgs, kv, cleanup
}

// BenchmarkColdStartReplay is the baseline for k3s-io/kine#720. The headline
// number is ns/op (equivalently the replay_s metric): the wall time for a fresh
// kine process to rebuild its index and declare itself ready.
func BenchmarkColdStartReplay(b *testing.B) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	p := loadBenchParams()
	ns, storeDir := setupReplayFixture(b, p)
	defer ns.Shutdown()
	_ = storeDir

	var (
		totalDur   time.Duration
		totalBytes uint64
		totalMsgs  uint64
		heapUsed   uint64
		indexKeys  int
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dur, inBytes, inMsgs, kv, cleanup := measureReplay(b, ns, p)

		b.StopTimer()
		totalDur += dur
		totalBytes += inBytes
		totalMsgs += inMsgs

		// Force collection so heap_MiB reports retained replay state rather
		// than setup garbage.
		//nolint:revive // This benchmark intentionally measures retained heap.
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heapUsed = ms.HeapAlloc
		indexKeys = kv.bt.Len()

		cleanup()
		b.StartTimer()
	}
	b.StopTimer()

	n := float64(b.N)
	b.ReportMetric(totalDur.Seconds()/n, "replay_s")
	b.ReportMetric(float64(totalBytes)/n/(1<<20), "wire_MiB")
	b.ReportMetric(float64(totalMsgs)/n, "wire_msgs")
	b.ReportMetric(float64(heapUsed)/(1<<20), "heap_MiB")
	b.ReportMetric(float64(indexKeys), "index_keys")
}

// BenchmarkSteadyState measures the per-operation cost once the index is warm,
// guarding against regressions introduced while optimizing cold start.
func BenchmarkSteadyState(b *testing.B) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	p := benchParams{keys: 500, revs: 4, valSize: 2048, history: 10}
	ns, _ := setupReplayFixture(b, p)
	defer ns.Shutdown()

	_, _, _, kv, cleanup := measureReplay(b, ns, p)
	defer cleanup()

	l := logrus.New()
	l.SetOutput(io.Discard)
	bk := &Backend{l: l, kv: kv}
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(2))
	value := benchValue(rnd, p.valSize)

	b.Run("Create", func(b *testing.B) {
		run := benchRun.Add(1)
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("/registry/bench/create/%d-%d", run, i)
			if _, err := bk.Create(ctx, key, value, 0); err != nil {
				b.Fatalf("create: %v", err)
			}
		}
	})

	b.Run("Update", func(b *testing.B) {
		key := fmt.Sprintf("/registry/bench/update/%d", benchRun.Add(1))
		rev, err := bk.Create(ctx, key, value, 0)
		if err != nil {
			b.Fatalf("create: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			newRev, _, ok, err := bk.Update(ctx, key, value, rev, 0)
			if err != nil || !ok {
				b.Fatalf("update: rev=%d ok=%v err=%v", rev, ok, err)
			}
			rev = newRev
		}
	})

	b.Run("Delete", func(b *testing.B) {
		run := benchRun.Add(1)
		keys := make([]string, b.N)
		revs := make([]int64, b.N)
		for i := 0; i < b.N; i++ {
			keys[i] = fmt.Sprintf("/registry/bench/delete/%d-%d", run, i)
			rev, err := bk.Create(ctx, keys[i], value, 0)
			if err != nil {
				b.Fatalf("create: %v", err)
			}
			revs[i] = rev
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, ok, err := bk.Delete(ctx, keys[i], revs[i]); err != nil || !ok {
				b.Fatalf("delete: ok=%v err=%v", ok, err)
			}
		}
	})

	b.Run("ListPrefix", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, kvs, err := bk.List(ctx, "/registry/pods/", "/registry/pods0", 0, 0, false); err != nil {
				b.Fatalf("list: %v", err)
			} else if len(kvs) == 0 {
				b.Fatal("list returned no keys")
			}
		}
	})

	b.Run("Count", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, _, err := bk.Count(ctx, "/registry/pods/", "/registry/pods0", 0); err != nil {
				b.Fatalf("count: %v", err)
			}
		}
	})
}
