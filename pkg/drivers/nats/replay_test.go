package nats

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

// newReplayFixture creates a bucket with a handful of keys already written, and
// returns a KeyValue that has not been started yet.
func newReplayFixture(t *testing.T, ctx context.Context, wg *sync.WaitGroup) (*KeyValue, jetstream.JetStream, func()) {
	t.Helper()

	p := benchParams{keys: 20, revs: 3, valSize: 128, history: 10}
	ns, _ := setupReplayFixture(t, p)

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)

	js, err := jetstream.New(nc)
	noErr(t, err)

	bkt, err := js.KeyValue(ctx, "kine")
	noErr(t, err)

	l := logrus.New()
	l.SetOutput(io.Discard)
	b := Backend{l: l}

	kv := NewKeyValue("replay-test", wg, bkt, js, int(p.history), b.Delete)
	b.kv = kv

	return kv, js, func() {
		nc.Close()
		ns.Shutdown()
	}
}

// TestReplayReadyAfterWatcherRestart pins the readiness contract that the crash
// loop in k3s-io/kine#720 depends on.
//
// btreeWatcher decides whether it is performing the startup replay by testing
// BucketRevision() == 0. KeyValue.Start restarts btreeWatcher after any consumer
// error, and by then lastSeq has advanced, so the restarted watcher concludes it
// is not the startup replay and never closes readyCh. waitReady then burns its
// whole budget and Backend.Start fails, which fails endpoint.Listen, which fails
// k3s startup — permanently, because every restart repeats it.
//
// A long replay over a large stream is exactly where a consumer reset is most
// likely, so this turns a slow replay into an unrecoverable one. Readiness must
// be signalled once the index has caught up to the stream, regardless of how
// many times the watcher had to restart to get there.
func TestReplayReadyAfterWatcherRestart(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()

	kv, _, stop := newReplayFixture(t, ctx, wg)
	defer stop()

	// Simulate the state Start() hands to a *restarted* btreeWatcher: the index
	// already holds part of the stream, so BucketRevision() is non-zero.
	kv.lastSeq.Store(1)

	kv.Start(ctx)

	select {
	case <-kv.readyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("readiness was never signalled after a watcher restart; " +
			"waitReady would fail and Backend.Start would abort")
	}
}

// TestReplayReadyOnEmptyBucket guards the other end of the same contract: a
// bucket with no messages must report ready promptly rather than waiting out the
// readiness timeout.
func TestReplayReadyOnEmptyBucket(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()

	ns := startBenchServer(t, t.TempDir())
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	noErr(t, err)

	bkt, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "kine", History: 10})
	noErr(t, err)

	l := logrus.New()
	l.SetOutput(io.Discard)
	b := Backend{l: l}
	kv := NewKeyValue("replay-test", wg, bkt, js, 10, b.Delete)
	b.kv = kv

	kv.Start(ctx)

	select {
	case <-kv.readyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("empty bucket never reported ready")
	}
}

// TestReplayIndexesLegacyDeletedKeys is the backward-compatibility gate for
// headers-only replay.
//
// Streams written before the Kine-Op header existed record a delete as an
// ordinary put whose payload says Delete=true, followed by a KV delete marker.
// A headers-only replay cannot read that payload, so it must recover the
// tombstone from the marker that follows it. This test writes the legacy pair
// by hand — no Kine-Op header — and asserts the rebuilt index hides the key,
// with List and Count agreeing.
//
// Without this recovery, every key ever deleted by an older kine would come back
// from the dead on the next restart.
func TestReplayIndexesLegacyDeletedKeys(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()

	ns := startBenchServer(t, t.TempDir())
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	noErr(t, err)

	bkt, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "kine", History: 10})
	noErr(t, err)
	noErr(t, ensureDirectGets(ctx, js, &Config{bucket: "kine"}))

	kc := &keyCodec{}
	vc := &valueCodec{}

	// Write the message pair exactly as a pre-header kine would have.
	writeLegacy := func(key string, del bool, hdr nats.Header) uint64 {
		ek, err := kc.Encode(key)
		noErr(t, err)

		var data []byte
		if hdr == nil {
			nd := natsData{
				Delete: del,
				Create: !del,
				KV:     &kserver.KeyValue{Key: key, Value: []byte("v")},
			}
			raw, err := nd.Encode()
			noErr(t, err)
			buf := new(bytes.Buffer)
			noErr(t, vc.Encode(raw, buf))
			data = buf.Bytes()
		}

		msg := &nats.Msg{
			Subject: "$KV.kine." + ek,
			Data:    data,
			Header:  hdr,
		}
		ack, err := js.PublishMsg(ctx, msg)
		noErr(t, err)
		return ack.Sequence
	}

	const kept = "/registry/legacy/kept"
	const gone = "/registry/legacy/gone"

	writeLegacy(kept, false, nil)
	liveRev := writeLegacy(gone, false, nil)
	// Legacy delete: tombstone put carrying Delete=true in the payload only...
	tombstoneRev := writeLegacy(gone, true, nil)
	// ...then the KV delete marker, pinned to the tombstone's sequence exactly as
	// Backend.Delete does via jetstream.LastRevision.
	writeLegacy(gone, false, nats.Header{
		"KV-Operation": []string{"DEL"},
		jetstream.ExpectedLastSubjSeqHeader: []string{
			strconv.FormatUint(tombstoneRev, 10),
		},
	})

	l := logrus.New()
	l.SetOutput(io.Discard)
	b := &Backend{l: l}
	kv := NewKeyValue("legacy", wg, bkt, js, 10, b.Delete)
	b.kv = kv

	kv.Start(ctx)
	select {
	case <-kv.readyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("index never reported ready")
	}

	const start = "/registry/legacy/"
	const end = "/registry/legacy0"

	_, kvs, err := b.List(ctx, start, end, 0, 0, false)
	noErr(t, err)
	for _, e := range kvs {
		if e == nil {
			t.Fatal("List returned a nil KeyValue")
		}
		if e.Key == gone {
			t.Fatalf("legacy-deleted key %s reappeared after headers-only replay", gone)
		}
	}
	if len(kvs) != 1 {
		t.Fatalf("expected 1 live key, got %d", len(kvs))
	}

	_, count, err := b.Count(ctx, start, end, 0)
	noErr(t, err)
	if count != int64(len(kvs)) {
		t.Fatalf("Count=%d disagrees with List=%d", count, len(kvs))
	}

	// The tombstone's own revision must also read as deleted, not as a live put
	// holding the pre-delete value.
	_, kvsAt, err := b.List(ctx, start, end, 0, int64(tombstoneRev), false)
	noErr(t, err)
	for _, e := range kvsAt {
		if e != nil && e.Key == gone {
			t.Fatalf("legacy tombstone at revision %d was treated as live", tombstoneRev)
		}
	}
	_, countAt, err := b.Count(ctx, start, end, int64(tombstoneRev))
	noErr(t, err)
	if countAt != int64(len(kvsAt)) {
		t.Fatalf("Count=%d disagrees with List=%d at revision %d", countAt, len(kvsAt), tombstoneRev)
	}

	// The revision *before* the delete must still show the key. Recovering
	// tombstones from the delete that follows them is easy to get backwards, in
	// which case the last live version is reclassified as deleted and the key
	// vanishes from every historical read — which is what the apiserver's
	// list-at-revision conformance tests catch.
	_, kvsLive, err := b.List(ctx, start, end, 0, int64(liveRev), false)
	noErr(t, err)
	found := false
	for _, e := range kvsLive {
		if e != nil && e.Key == gone {
			found = true
		}
	}
	if !found {
		t.Fatalf("key %s should still be live at revision %d, before its delete", gone, liveRev)
	}
	_, countLive, err := b.Count(ctx, start, end, int64(liveRev))
	noErr(t, err)
	if countLive != int64(len(kvsLive)) {
		t.Fatalf("Count=%d disagrees with List=%d at revision %d", countLive, len(kvsLive), liveRev)
	}
}

// TestReplayIndexesDeletedKeys pins the visibility contract that any change to
// how replay reconstructs the index must preserve: a key whose last operation
// was a delete must be invisible to List and Count at the current revision, and
// must also be invisible when queried at the revision of its own tombstone.
//
// This matters because pkg/server/list.go pairs a List with a Count at the same
// revision to compute the "More" flag for apiserver pagination; if the two
// disagree about whether a tombstoned key is visible, pagination breaks.
func TestReplayIndexesDeletedKeys(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()

	ns := startBenchServer(t, t.TempDir())
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	noErr(t, err)

	bkt, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "kine", History: 10})
	noErr(t, err)
	noErr(t, ensureDirectGets(ctx, js, &Config{bucket: "kine"}))

	l := logrus.New()
	l.SetOutput(io.Discard)
	b := &Backend{l: l}
	kv := NewKeyValue("replay-test", wg, bkt, js, 10, b.Delete)
	b.kv = kv
	noErr(t, b.Start(ctx))

	const kept = "/registry/things/kept"
	const gone = "/registry/things/gone"

	_, err = b.Create(ctx, kept, []byte("a"), 0)
	noErr(t, err)
	goneRev, err := b.Create(ctx, gone, []byte("b"), 0)
	noErr(t, err)

	delRev, _, ok, err := b.Delete(ctx, gone, goneRev)
	noErr(t, err)
	if !ok {
		t.Fatal("delete did not report success")
	}

	// Rebuild the index from the stream, exactly as a restarted process would.
	kv.Stop()
	wg.Wait()

	kv2 := NewKeyValue("replay-test-2", wg, bkt, js, 10, b.Delete)
	b2 := &Backend{l: l, kv: kv2}
	kv2.Start(ctx)
	select {
	case <-kv2.readyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("rebuilt index never reported ready")
	}

	const start = "/registry/things/"
	const end = "/registry/things0"

	// At the current revision the deleted key must be gone, and List and Count
	// must agree.
	_, kvs, err := b2.List(ctx, start, end, 0, 0, false)
	noErr(t, err)
	for _, e := range kvs {
		if e == nil {
			t.Fatal("List returned a nil KeyValue")
		}
		if e.Key == gone {
			t.Fatalf("deleted key %s was returned by List after replay", gone)
		}
	}
	if len(kvs) != 1 {
		t.Fatalf("expected 1 live key after replay, got %d", len(kvs))
	}

	_, count, err := b2.Count(ctx, start, end, 0)
	noErr(t, err)
	if count != int64(len(kvs)) {
		t.Fatalf("Count=%d disagrees with List=%d at current revision", count, len(kvs))
	}

	// At the tombstone's own revision the key is already deleted, so it must not
	// be visible there either, and Count must still agree with List.
	_, kvsAt, err := b2.List(ctx, start, end, 0, delRev, false)
	noErr(t, err)
	for _, e := range kvsAt {
		if e != nil && e.Key == gone {
			t.Fatalf("deleted key %s was returned by List at its tombstone revision %d", gone, delRev)
		}
	}
	_, countAt, err := b2.Count(ctx, start, end, delRev)
	noErr(t, err)
	if countAt != int64(len(kvsAt)) {
		t.Fatalf("Count=%d disagrees with List=%d at revision %d", countAt, len(kvsAt), delRev)
	}

	// ...but it must still be visible at the revision it was created at.
	_, kvsLive, err := b2.List(ctx, start, end, 0, goneRev, false)
	noErr(t, err)
	found := false
	for _, e := range kvsLive {
		if e != nil && e.Key == gone {
			found = true
		}
	}
	if !found {
		t.Fatalf("key %s should still be live at revision %d, before its delete", gone, goneRev)
	}
}

// TestReplayTimeoutIsHonoured pins the readiness timeout to the replayTimeout
// query parameter. k3s-io/kine#720 asked for exactly this knob, and it is easy
// for the config to stay plumbed while waitReady quietly keeps its own constant,
// which makes the parameter look supported while doing nothing.
func TestReplayTimeoutIsHonoured(t *testing.T) {
	logrus.SetLevel(logrus.PanicLevel)
	logrus.SetOutput(io.Discard)

	cfg, err := parseConnection("nats://localhost:4222?replayTimeout=1500ms", tls.Config{})
	noErr(t, err)
	if cfg.replayTimeout != 1500*time.Millisecond {
		t.Fatalf("replayTimeout not parsed: got %s", cfg.replayTimeout)
	}

	if _, err := parseConnection("nats://localhost:4222?replayTimeout=nonsense", tls.Config{}); err == nil {
		t.Fatal("expected an error for an unparseable replayTimeout")
	}
	if _, err := parseConnection("nats://localhost:4222?replayTimeout=0s", tls.Config{}); err == nil {
		t.Fatal("expected an error for a zero replayTimeout")
	}

	// waitReady must actually use the configured value rather than its own
	// constant: with readiness never signalled, it should give up promptly.
	kv := &KeyValue{name: "t", readyCh: make(chan struct{})}
	kv.SetReplayTimeout(300 * time.Millisecond)

	start := time.Now()
	err = kv.waitReady(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("waitReady ignored the configured timeout: waited %s", elapsed)
	}
	if !strings.Contains(err.Error(), "replayTimeout") {
		t.Fatalf("timeout error should name the knob to raise; got: %v", err)
	}
}
