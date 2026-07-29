package nats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

var errStopKeyValue = errors.New("stopping key value")

const (
	// kineOpHeader marks a KV put that is really a kine tombstone. kine records
	// a delete as a normal put whose payload carries Delete=true, so without
	// this header the only way to classify a message is to fetch and decode its
	// payload — which is what made index replay pull the entire bucket over the
	// network and JSON-decode all of it. Older streams have no such header; see
	// btreeWatcher for how their tombstones are recovered.
	kineOpHeader = "Kine-Op"
	kineOpDelete = "DEL"

	kvOperationHeader = "KV-Operation"

	// replayProgressInterval is how often an in-progress index replay logs.
	replayProgressInterval = 10 * time.Second
)

type keySeq struct {
	key string
	seq uint64
}

type entry struct {
	kc *keyCodec
	vc *valueCodec
	// key holds the already-decoded key when the producer of this entry had it
	// to hand, avoiding a second base58 decode of the same subject.
	key string
	// tombstone reports that this put carried the kine tombstone header, known
	// without reading the payload. Only set by the Watch handler.
	tombstone bool
	// prevSeq is the sequence this message was written against
	// (Nats-Expected-Last-Subject-Sequence), or zero if unconditional. Only set
	// by the Watch handler.
	prevSeq uint64
	entry   jetstream.KeyValueEntry
}

func (e *entry) Key() string {
	if e.key != "" {
		return e.key
	}

	dk, err := e.kc.Decode(e.entry.Key())
	// should not happen
	if err != nil {
		// should not happen
		logrus.Warnf("could not decode key %s: %v", e.entry.Key(), err)
		return ""
	}

	return dk
}

func (e *entry) Bucket() string { return e.entry.Bucket() }
func (e *entry) Value() []byte {
	buf := new(bytes.Buffer)
	if err := e.vc.Decode(bytes.NewBuffer(e.entry.Value()), buf); err != nil {
		// should not happen
		logrus.Warnf("could not decode value for %s: %v", e.Key(), err)
	}
	return buf.Bytes()
}
func (e *entry) Revision() uint64                { return e.entry.Revision() }
func (e *entry) Created() time.Time              { return e.entry.Created() }
func (e *entry) Delta() uint64                   { return e.entry.Delta() }
func (e *entry) Operation() jetstream.KeyValueOp { return e.entry.Operation() }

type seqOp struct {
	seq uint64
	op  jetstream.KeyValueOp
}

type streamWatcher struct {
	con        jetstream.Consumer
	cctx       jetstream.ConsumeContext
	keyCodec   *keyCodec
	valueCodec *valueCodec
	updates    chan jetstream.KeyValueEntry
	ctx        context.Context
	errch      chan error
}

func (w *streamWatcher) Context() context.Context {
	if w == nil {
		return nil
	}
	return w.ctx
}

func (w *streamWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return w.updates
}

func (w *streamWatcher) Err() <-chan error {
	return w.errch
}

func (w *streamWatcher) Stop() error {
	if w.cctx != nil {
		w.cctx.Stop()
	}
	return nil
}

type kvEntry struct {
	key       string
	bucket    string
	value     []byte
	revision  uint64
	created   time.Time
	delta     uint64
	operation jetstream.KeyValueOp
}

func (e *kvEntry) Key() string {
	return e.key
}

func (e *kvEntry) Bucket() string { return e.bucket }
func (e *kvEntry) Value() []byte {
	return e.value
}
func (e *kvEntry) Revision() uint64                { return e.revision }
func (e *kvEntry) Created() time.Time              { return e.created }
func (e *kvEntry) Delta() uint64                   { return e.delta }
func (e *kvEntry) Operation() jetstream.KeyValueOp { return e.operation }

type KeyValue struct {
	name       string
	revHistory int
	nkv        jetstream.KeyValue
	js         jetstream.JetStream
	kc         *keyCodec
	vc         *valueCodec
	bt         *btree.Map[string, []*seqOp]
	ew         *ExpireWatcher
	btm        sync.RWMutex
	lastSeq    atomic.Uint64
	compactRev atomic.Int64
	readyCh    chan struct{}
	ready      atomic.Bool
	seqCond    *sync.Cond   // Broadcasts on btree update for read-after-write consistency
	seqWaiters atomic.Int64 // Number of waiters in waitForSequence
	// replayTimeout bounds how long Start blocks waiting for the index to catch
	// up with the stream before giving up.
	replayTimeout time.Duration
	ctx           context.Context
	cancel        context.CancelCauseFunc
	wg            *sync.WaitGroup
}

func NewKeyValue(name string, wg *sync.WaitGroup, bucket jetstream.KeyValue, js jetstream.JetStream, revHistory int, deleteFn DeleteFn) *KeyValue {
	kv := &KeyValue{
		name:          name,
		revHistory:    revHistory,
		nkv:           bucket,
		js:            js,
		kc:            &keyCodec{},
		vc:            &valueCodec{},
		bt:            btree.NewMap[string, []*seqOp](0),
		ew:            NewExpireWatcher(deleteFn),
		seqCond:       sync.NewCond(&sync.Mutex{}),
		readyCh:       make(chan struct{}),
		replayTimeout: defaultReplayTimeout,
		wg:            wg,
	}

	return kv
}

// SetReplayTimeout overrides how long Start waits for the index to catch up with
// the stream. Must be called before Start.
func (e *KeyValue) SetReplayTimeout(d time.Duration) {
	if d > 0 {
		e.replayTimeout = d
	}
}

func (e *KeyValue) Start(ctx context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)

	e.ctx = ctx
	e.cancel = cancel

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		for {
			errch := make(chan error, 1)

			go func() {
				errch <- e.btreeWatcher(ctx, e.revHistory)
			}()

			var err error
			select {
			case err = <-errch:
				if !errors.Is(err, jetstream.ErrServerShutdown) && !errors.Is(err, jetstream.ErrConnectionClosed) {
					logrus.Errorf("btree watcher: error: %v", err)
				}

			case <-ctx.Done():
				logrus.Infof("%s: stopping key value store", e.name)
				err = context.Cause(ctx)
				// Parent context was canceled
				if err != errStopKeyValue {
					e.js.Conn().Close()
				}
				return
			}

			logrus.Debugf("%s: btree watcher: %v", e.name, err)

			if errors.Is(err, errStopKeyValue) {
				return
			}

			if errors.Is(err, nats.ErrConnectionClosed) {
				return
			}

			jitterSleep(time.Second)
		}
	}()
}

func (e *KeyValue) GetRevision(ctx context.Context, key string, revision int64, checkRevision bool) (jetstream.KeyValueEntry, error) {
	if revision < 0 {
		logrus.Warnf("getRevision: key=%s, revision=%d is less than 0, setting to 0", key, revision)
		revision = 0
	}

	if checkRevision {
		err := e.checkRevision(key, revision)
		if err != nil {
			return nil, err
		}
	}

	op, err := e.getRevisionOp(key, revision, false)
	if err != nil {
		return nil, err
	}

	if op == nil {
		return nil, jetstream.ErrKeyNotFound
	}

	if revision > 0 {
		revision = int64(op.seq)
	}

	return e.getRevision(ctx, key, revision)
}

func (e *KeyValue) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.vc.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	rev, err := e.nkv.Create(ctx, ek, buf.Bytes())
	if err != nil {
		return rev, err
	}

	// Wait for btree watcher to process this sequence for read-after-write consistency
	if err := e.waitForSequence(ctx, rev, waitForSeqTimeout); err != nil {
		logrus.Warnf("create: btree watcher lag: key=%s, seq=%d, err=%v", key, rev, err)
		// Continue anyway - data is in NATS, btree is lagging
	}

	return rev, nil
}

func (e *KeyValue) Update(ctx context.Context, key string, value []byte, last uint64) (uint64, error) {
	return e.update(ctx, key, value, last, false)
}

// UpdateTombstone writes a kine delete tombstone. It is identical to Update
// except that the message is tagged so the index can recognize it from headers
// alone.
func (e *KeyValue) UpdateTombstone(ctx context.Context, key string, value []byte, last uint64) (uint64, error) {
	return e.update(ctx, key, value, last, true)
}

func (e *KeyValue) update(ctx context.Context, key string, value []byte, last uint64, tombstone bool) (uint64, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return 0, err
	}

	buf := new(bytes.Buffer)

	err = e.vc.Encode(value, buf)
	if err != nil {
		return 0, err
	}

	var rev uint64
	if tombstone {
		rev, err = e.publishTombstone(ctx, ek, buf.Bytes(), last)
	} else {
		rev, err = e.nkv.Update(ctx, ek, buf.Bytes(), last)
	}
	if err != nil {
		return rev, err
	}

	// Wait for btree watcher to process this sequence for read-after-write consistency
	if err := e.waitForSequence(ctx, rev, waitForSeqTimeout); err != nil {
		logrus.Warnf("create: btree watcher lag: key=%s, seq=%d, err=%v", key, rev, err)
		// Continue anyway - data is in NATS, btree is lagging
	}

	return rev, nil
}

// publishTombstone performs the same publish as jetstream's KeyValue.Update —
// same subject, same expected-last-subject-sequence CAS, so the same
// JSErrCodeStreamWrongLastSequence surfaces on conflict — with the kine
// tombstone header attached. nats.go's KeyValue API has no way to pass headers.
func (e *KeyValue) publishTombstone(ctx context.Context, encodedKey string, value []byte, last uint64) (uint64, error) {
	msg := nats.NewMsg(fmt.Sprintf("$KV.%s.%s", e.nkv.Bucket(), encodedKey))
	msg.Data = value
	msg.Header.Set(kineOpHeader, kineOpDelete)

	ack, err := e.js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(last))
	if err != nil {
		return 0, err
	}

	return ack.Sequence, nil
}

func (e *KeyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return err
	}

	err = e.nkv.Delete(ctx, ek, opts...)
	if err != nil {
		return err
	}

	kvs, err := e.nkv.History(ctx, ek, jetstream.MetaOnly())
	if err != nil {
		logrus.Errorf("delete: error getting history for key %s: %v", key, err)
	}

	if len(kvs) > 0 {
		last := kvs[len(kvs)-1]
		if last.Operation() == jetstream.KeyValueDelete {
			if err := e.waitForSequence(ctx, last.Revision(), waitForSeqTimeout); err != nil {
				logrus.Warnf("delete: btree watcher lag: key=%s, seq=%d, err=%v", key, last.Revision(), err)
				// Continue anyway - data is in NATS, btree is lagging
			}
		} else {
			logrus.Warnf("delete: key was not deleted, last operation was key=%s, op=%s, seq=%d", key, last.Operation(), last.Revision())
		}
	}

	return nil
}

type KeyWatcher interface {
	Updates() <-chan jetstream.KeyValueEntry
	Stop() error
	Err() <-chan error
}

// WatchOpt configures a Watch.
type WatchOpt func(*watchOpts)

type watchOpts struct {
	headersOnly bool
}

// WithHeadersOnly asks the server to deliver message headers without payloads.
// Only useful for consumers that classify messages rather than read their
// values — the index replay, which would otherwise pull the whole bucket.
func WithHeadersOnly() WatchOpt {
	return func(o *watchOpts) { o.headersOnly = true }
}

func (e *KeyValue) Watch(ctx context.Context, key, end string, startRev int64, opts ...WatchOpt) (KeyWatcher, error) {
	var wo watchOpts
	for _, opt := range opts {
		opt(&wo)
	}

	// Everything but the last token will be treated as a filter
	// on the watcher. The last token will used as a deliver-time filter.
	filter := key

	if filter != "/" && strings.HasSuffix(filter, "/") {
		filter = strings.TrimSuffix(key, "/")
	} else {
		idx := strings.LastIndexByte(filter, '/')
		if idx > -1 {
			filter = key[:idx+1]
		} else {
			// No '/' prefix in key and no '/' within key.
			// We should subscribe on the meta subject
			filter = noRootPrefix
		}
	}

	if filter != "" {
		p, err := e.kc.EncodeRange(filter)
		if err != nil {
			return nil, err
		}

		filter = fmt.Sprintf("$KV.%s.%s", e.nkv.Bucket(), p)
	}

	updates := make(chan jetstream.KeyValueEntry, 100)
	subjectPrefix := fmt.Sprintf("$KV.%s.", e.nkv.Bucket())

	handler := func(msg jetstream.Msg) {
		md, _ := msg.Metadata()

		if md.Sequence.Stream < uint64(e.compactRev.Load()) {
			return
		}

		// Decode once and carry the result on the entry. keyCodec.Decode is
		// base58 big-integer math per '/'-separated token, and the consumer of
		// this channel needs the same decoded key, so decoding it twice per
		// message was a material share of replay cost.
		var dkey string
		skey := strings.TrimPrefix(msg.Subject(), subjectPrefix)
		if skey != "" {
			var err error
			dkey, err = e.kc.Decode(strings.TrimPrefix(skey, "."))
			if err != nil || (key == "" && dkey < key) || (end != "" && dkey >= end) {
				return
			}
		}

		// Default is PUT
		var op jetstream.KeyValueOp
		hdr := msg.Headers()
		switch hdr.Get(kvOperationHeader) {
		case "DEL":
			op = jetstream.KeyValueDelete
		case "PURGE":
			op = jetstream.KeyValuePurge
		}
		// Not currently used...
		delta := 0

		// Reported separately from Operation: Backend.Watch must still see a
		// tombstone as a PUT so it can emit a delete event from the payload.
		tombstone := op == jetstream.KeyValuePut && hdr.Get(kineOpHeader) == kineOpDelete

		// The sequence this write was conditioned on, used to identify the
		// message a delete marker supersedes.
		var prevSeq uint64
		if v := hdr.Get(jetstream.ExpectedLastSubjSeqHeader); v != "" {
			prevSeq, _ = strconv.ParseUint(v, 10, 64)
		}

		updates <- &entry{
			kc:        e.kc,
			vc:        e.vc,
			key:       dkey,
			tombstone: tombstone,
			prevSeq:   prevSeq,
			entry: &kvEntry{
				key:       skey,
				bucket:    e.nkv.Bucket(),
				value:     msg.Data(),
				revision:  md.Sequence.Stream,
				created:   md.Timestamp,
				delta:     uint64(delta),
				operation: op,
			},
		}
	}

	var dp jetstream.DeliverPolicy
	var cfg jetstream.OrderedConsumerConfig
	if startRev <= 0 {
		dp = jetstream.DeliverAllPolicy
	} else {
		dp = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = uint64(startRev)
	}
	cfg.DeliverPolicy = dp
	cfg.FilterSubjects = append(cfg.FilterSubjects, filter)
	cfg.HeadersOnly = wo.headersOnly

	con, err := e.js.OrderedConsumer(ctx, fmt.Sprintf("KV_%s", e.nkv.Bucket()), cfg)
	if err != nil {
		return nil, errors.Join(errors.New("watch: creating ordered consumer"), err)
	}

	errch := make(chan error, 1)
	ci := con.CachedInfo()
	cctx, err := con.Consume(handler,
		jetstream.ConsumeErrHandler(func(cctx jetstream.ConsumeContext, err error) {
			errch <- fmt.Errorf("watch: error consuming from %s: %w", ci.Name, err)
		}),
	)
	if err != nil {
		return nil, errors.Join(errors.New("watch: starting consuming"), err)
	}

	w := &streamWatcher{
		con:        con,
		cctx:       cctx,
		keyCodec:   e.kc,
		valueCodec: e.vc,
		updates:    updates,
		ctx:        ctx,
		errch:      errch,
	}

	return w, nil
}

// BucketSize returns the size of the bucket in bytes.
func (e *KeyValue) BucketSize(ctx context.Context) (int64, error) {
	status, err := e.nkv.Status(ctx)
	if err != nil {
		return 0, err
	}
	return int64(status.Bytes()), nil
}

// BucketRevision returns the latest revision of the bucket.
func (e *KeyValue) BucketRevision() int64 {
	return int64(e.lastSeq.Load())
}

func (e *KeyValue) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) ([]jetstream.KeyValueEntry, error) {
	err := e.checkRevision("", revision)
	if err != nil {
		return nil, err
	}

	var matches []*keySeq

	// btree.Map is not safe for concurrent use and btreeWatcher calls Set
	// continuously, so the iterator must be created and positioned under the
	// same lock that guards the walk.
	e.btm.RLock()
	it := e.bt.Iter()
	seeked := true
	if key != "" {
		seeked = it.Seek(key)
	}
	for seeked {
		k := it.Key()
		if (end == "" && k != key) || (end != "" && k >= end) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if op := getSeqOp(v, revision, false); op != nil {
			matches = append(matches, &keySeq{key: k, seq: op.seq})
		}

		if limit > 0 && int64(len(matches)) >= limit {
			break
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	if !seeked {
		return nil, nil
	}

	var entries []jetstream.KeyValueEntry
	for _, m := range matches {
		valueEntry, err := e.getRevision(ctx, m.key, int64(m.seq))
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}

		entries = append(entries, valueEntry)
	}

	return entries, nil
}

func (e *KeyValue) Count(ctx context.Context, key, end string, revision int64) (int64, error) {
	matches, err := e.getListOps(key, end, revision)
	if err != nil {
		return 0, err
	}

	return int64(len(matches)), nil
}

func (e *KeyValue) Stop() {
	e.cancel(errStopKeyValue)
	<-e.ctx.Done()
}

// waitForSequence waits for the btree watcher to consume up to the given sequence number.
// This ensures read-after-write consistency by blocking until the async watcher has
// updated the btree with the written data.
func (e *KeyValue) waitForSequence(ctx context.Context, seq uint64, timeout time.Duration) error {
	// Fast path: check if already processed
	if e.lastSeq.Load() >= seq {
		return nil
	}

	done := make(chan struct{}) // Closed when condition is met
	stop := make(chan struct{}) // Closed when we should abort waiting

	// Registered before the watcher's lastSeq re-check below, so the watcher
	// either observes this waiter and broadcasts, or has already advanced
	// lastSeq past seq and the re-check returns immediately.
	e.seqWaiters.Add(1)
	defer e.seqWaiters.Add(-1)

	go func() {
		e.seqCond.L.Lock()
		defer e.seqCond.L.Unlock()

		for e.lastSeq.Load() < seq {
			select {
			case <-stop:
				return
			default:
			}

			e.seqCond.Wait() // Releases lock, waits for broadcast, reacquires lock
		}
		close(done)
	}()

	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case <-done:
		return nil
	case <-t.C:
		close(stop)           // Signal goroutine to exit
		e.seqCond.Broadcast() // Wake up Wait() so it can check stop
		return fmt.Errorf("timeout waiting for sequence %d", seq)
	case <-ctx.Done():
		close(stop)           // Signal goroutine to exit
		e.seqCond.Broadcast() // Wake up Wait() so it can check stop
		return ctx.Err()
	}
}

// markReady signals that the index has caught up with the stream. It is safe to
// call from any watcher attempt and more than once.
func (e *KeyValue) markReady() {
	if e.ready.CompareAndSwap(false, true) {
		close(e.readyCh)
	}
}

// waitReady waits for the btree watcher to finish replaying history on startup.
// This prevents reads from seeing incomplete/inconsistent state during cold start.
func (e *KeyValue) waitReady(ctx context.Context) error {
	timeout := time.NewTimer(e.replayTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			// Report how far replay got, so a slow replay can be told apart from
			// a stuck one and the operator knows what to raise replayTimeout to.
			return fmt.Errorf("timeout waiting for btree to be ready after %s (reached revision %d); "+
				"raise it with the replayTimeout query parameter on the datastore endpoint",
				e.replayTimeout, e.BucketRevision())
		case <-ctx.Done():
			return ctx.Err()
		case <-e.readyCh:
			logrus.Infof("%s: stream replay complete, ready for operations", e.name)
			return nil
		}
	}
}

// getRevision returns the NATS key value entry for the given key and revision.
// Expects valid NATS KV revision for key or zero for latest
func (e *KeyValue) getRevision(ctx context.Context, key string, revision int64) (jetstream.KeyValueEntry, error) {
	ek, err := e.kc.Encode(key)
	if err != nil {
		return nil, err
	}

	var ent jetstream.KeyValueEntry

	if revision <= 0 {
		ent, err = e.nkv.Get(ctx, ek)
	} else {
		ent, err = e.nkv.GetRevision(ctx, ek, uint64(revision))
	}
	if err != nil {
		return nil, err
	}

	return &entry{
		kc:    e.kc,
		vc:    e.vc,
		entry: ent,
	}, nil
}

func (e *KeyValue) checkRevision(key string, revision int64) error {
	if key != "" {
		e.btm.RLock()
		_, ok := e.bt.Get(key)
		if !ok {
			e.btm.RUnlock()
			return jetstream.ErrKeyNotFound
		}
		e.btm.RUnlock()
	}

	if revision > 0 {
		currRev := e.BucketRevision()
		if revision > currRev {
			return server.ErrFutureRev
		}

		compactRev := e.compactRev.Load()

		if revision < compactRev {
			return server.ErrCompacted
		}
	}

	return nil
}

func (e *KeyValue) btreeWatcher(ctx context.Context, hsize int) error {
	br := e.BucketRevision()

	s, err := e.js.Stream(ctx, fmt.Sprintf("KV_%s", e.nkv.Bucket()))
	if err != nil {
		return fmt.Errorf("failed to get stream info: %w", err)
	}
	targetSeq := s.CachedInfo().State.LastSeq

	// Readiness must track whether the index has caught up with the stream, not
	// whether this is the first attempt. Start() restarts this watcher after any
	// consumer error, and by then lastSeq has advanced, so keying off br == 0
	// left a restarted watcher unable to ever signal readiness — waitReady then
	// burned its whole budget and Backend.Start aborted, permanently, since every
	// process restart repeats it. A long replay is exactly where a consumer reset
	// is most likely. See TestReplayReadyAfterWatcherRestart.
	replaying := !e.ready.Load()

	if replaying && uint64(br) >= targetSeq {
		// The bucket is empty, or the index already covers the stream. No further
		// message is guaranteed to arrive to trigger the catch-up check below.
		e.markReady()
		replaying = false
	} else if replaying {
		logrus.Infof("%s: starting initial replay from %d to %d", e.name, br, targetSeq)
	}

	now := time.Now()
	lastProgress := now
	// The index stores only a sequence and an operation per revision, so the
	// payloads are pure waste on this consumer — at the scale of k3s-io/kine#720
	// that was 1.3 GiB pulled from the cluster on every process start.
	w, err := e.Watch(ctx, "/", "", br, WithHeadersOnly())
	if err != nil {
		return fmt.Errorf("init: %s after %s", err, time.Since(now))
	}
	defer w.Stop()

	for {
		select {
		case err := <-w.Err():
			return fmt.Errorf("error: %w", err)

		case x := <-w.Updates():
			if x == nil {
				continue
			}

			seq := x.Revision()
			op := x.Operation()

			key := x.Key()

			// Whether this message is a real KV delete marker, as opposed to a
			// kine tombstone that merely reads as a delete. Captured before the
			// tombstone remap below, because only a real marker may retroactively
			// reclassify the message before it.
			delMarker := op == jetstream.KeyValueDelete

			// A kine delete is a put whose payload says Delete=true. Messages
			// written by this version say so in a header instead, so no payload
			// is needed to classify them.
			var supersedes uint64
			if ent, ok := x.(*entry); ok {
				if ent.tombstone {
					op = jetstream.KeyValueDelete
				}
				supersedes = ent.prevSeq
			}

			e.btm.Lock()

			val, ok := e.bt.Get(key)
			if !ok {
				val = make([]*seqOp, 0, hsize)
			}

			// Tombstones on streams written before the header existed carry no
			// marker, so recover them from the delete that follows: kine writes a
			// tombstone put and then a KV delete marker pinned to the tombstone's
			// sequence via expected-last-subject-sequence, so the marker's
			// predecessor on that subject is the tombstone — and this per-key list
			// is exactly that subject's message list.
			//
			// Known exception: kine before 52aa8b8 (2025-11-07) deleted
			// lease-expired keys with a bare delete marker pinned to the *live*
			// value's revision, with no tombstone put. On a stream that old, the
			// last live version of an expired key is reclassified as deleted at its
			// own revision. It is deleted at the current revision either way, so
			// this only affects a read at exactly that historical revision, and
			// only until the key is written again by a version that sets the
			// header. The alternative — not recovering legacy tombstones at all —
			// misreports every key deleted by current kine, which is worse.
			//
			// Matching is by sequence rather than by list position: the Watch
			// handler drops messages below the compaction revision, so the last
			// entry recorded for a key is not always the message the marker
			// supersedes. A marker with no expected-sequence header did not come
			// from kine and reclassifies nothing.
			if delMarker && supersedes > 0 {
				for i := len(val) - 1; i >= 0 && val[i].seq >= supersedes; i-- {
					if val[i].seq == supersedes {
						if val[i].op == jetstream.KeyValuePut {
							val[i].op = jetstream.KeyValueDelete
						}
						break
					}
				}
			}

			// Remove the oldest entry.
			if len(val) == cap(val) {
				val = append(val[:0], val[1:]...)
			}

			val = append(val, &seqOp{
				seq: seq,
				op:  op,
			})

			e.bt.Set(key, val)

			e.lastSeq.Store(seq)

			e.btm.Unlock()

			// Broadcast to all waiters that sequence has advanced. During a cold
			// start there are no writers waiting, so skipping the broadcast
			// avoids a futex wake per replayed message. seqWaiters is incremented
			// before the waiter re-checks lastSeq under seqCond.L, so a waiter
			// that arrives concurrently either sees the new lastSeq itself or is
			// counted here.
			if e.seqWaiters.Load() > 0 {
				e.seqCond.Broadcast()
			}

			// Check if startup replay is complete
			if replaying {
				if seq >= targetSeq {
					e.markReady()
					replaying = false
					logrus.Infof("%s: btree replay complete at seq=%d (target was %d, took %s)",
						e.name, seq, targetSeq, time.Since(now))
				} else if time.Since(lastProgress) >= replayProgressInterval {
					// Without this, a slow replay is indistinguishable from a hung
					// one, which is how k3s-io/kine#720 was diagnosed as a hang.
					lastProgress = time.Now()
					logrus.Infof("%s: btree replay at seq=%d of %d (%d keys indexed, %s elapsed)",
						e.name, seq, targetSeq, e.bt.Len(), time.Since(now).Round(time.Second))
				}
			}
		}
	}
}

func (e *KeyValue) getListOps(key, end string, revision int64) ([]*keySeq, error) {
	err := e.checkRevision("", revision)
	if err != nil {
		return nil, err
	}

	var matches []*keySeq

	// See KeyValue.List: the iterator must be created and positioned under the
	// same lock that guards the walk.
	e.btm.RLock()
	it := e.bt.Iter()
	seeked := true
	if key != "" {
		seeked = it.Seek(key)
	}
	for seeked {
		k := it.Key()
		if (end == "" && k != key) || (end != "" && k >= end) {
			break
		}

		v := it.Value()

		// Get the latest update for the key.
		if op := getSeqOp(v, revision, false); op != nil {
			matches = append(matches, &keySeq{key: k, seq: op.seq})
		}

		if !it.Next() {
			break
		}
	}
	e.btm.RUnlock()

	if !seeked {
		return nil, nil
	}

	return matches, nil
}

// getRevisionOp returns the latest btree operation for the requested revision
func (e *KeyValue) getRevisionOp(key string, revision int64, allowDeleted bool) (*seqOp, error) {
	e.btm.RLock()
	val, ok := e.bt.Get(key)
	if !ok {
		e.btm.RUnlock()
		return nil, jetstream.ErrKeyNotFound
	}
	e.btm.RUnlock()

	op := getSeqOp(val, revision, allowDeleted)

	if op == nil {
		return nil, jetstream.ErrKeyNotFound
	}

	return op, nil
}

// getSeqOp returns the latest sequence operation for the given key and global revision
func getSeqOp(val []*seqOp, revision int64, allowDeleted bool) *seqOp {
	for i := len(val) - 1; i >= 0; i-- {
		op := val[i]
		if revision <= 0 || op.seq <= uint64(revision) {
			if op.op != jetstream.KeyValuePut && !allowDeleted {
				return nil
			}

			return op
		}
	}

	return nil
}
