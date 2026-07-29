package nats

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/klauspost/compress/s2"
)

func TestKeyEncode(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "", true},
		{"/", "", true},
		{"a", fmt.Sprintf("%s.2g", noRootPrefix), false},
		{"/a/a", "2g.2g", false},
		{"a/a", fmt.Sprintf("%s.2g.2g", noRootPrefix), false},
		{"/a/a/a", "2g.2g.2g", false},
		{"a/*/a", fmt.Sprintf("%s.2g.j.2g", noRootPrefix), false},
		{"/a/*/a/", "2g.j.2g.p", false},
	}

	codec := &keyCodec{}

	for _, test := range tests {
		out, err := codec.Encode(test.In)
		if err != nil {
			if !test.Err {
				t.Errorf("Expected no error for %q, got %v", test.In, err)
			}
			continue
		}
		if out != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}

func TestKeyDecode(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "/", false},
		{"2g", "/a", false},
		{"2g.2g", "/a/a", false},
		{"2g.2g.2g", "/a/a/a", false},
	}

	codec := &keyCodec{}

	for _, test := range tests {
		out, err := codec.Decode(test.In)
		if err != nil {
			if !test.Err {
				t.Errorf("Expected no error for %q, got %v", test.In, err)
			}
			continue
		}
		if out != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}

func TestKeyEncodeRange(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "", true},
		{"/", ">", false},
		{"a", fmt.Sprintf("%s.2g.>", noRootPrefix), false},
		{"/a/a", "2g.2g.>", false},
		{"a/a/a", fmt.Sprintf("%s.2g.2g.2g.>", noRootPrefix), false},
		{"/a/*/a", "2g.j.2g.>", false},
		{"a/*/a", fmt.Sprintf("%s.2g.j.2g.>", noRootPrefix), false},
	}

	codec := &keyCodec{}

	for _, test := range tests {
		out, err := codec.EncodeRange(test.In)
		if err != nil {
			if !test.Err {
				t.Errorf("Expected no error for %q, got %v", test.In, err)
			}
			continue
		}
		if out != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}

// TestValueCodecRoundTrip pins the on-the-wire compatibility of the value codec.
//
// Encode runs on every write, so a change to how it frames its output is
// irreversible once published to a stream. Pooling the s2 writer (and pinning it
// to single-block concurrency) must not change what a reader sees: bytes written
// by this version must decode with a stock s2 reader, and bytes written by a
// previous version — a stock s2 writer — must decode here.
func TestValueCodecRoundTrip(t *testing.T) {
	vc := &valueCodec{}

	sizes := []int{0, 1, 100, 4096, 1 << 20, (1 << 20) + 13}
	for _, n := range sizes {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte('a' + i%26)
		}

		// New writer -> new reader.
		enc := new(bytes.Buffer)
		if err := vc.Encode(src, enc); err != nil {
			t.Fatalf("size %d: encode: %v", n, err)
		}
		dec := new(bytes.Buffer)
		if err := vc.Decode(bytes.NewReader(enc.Bytes()), dec); err != nil {
			t.Fatalf("size %d: decode: %v", n, err)
		}
		if !bytes.Equal(dec.Bytes(), src) {
			t.Fatalf("size %d: round trip mismatch", n)
		}

		// New writer -> stock s2 reader (an older kine reading our bytes).
		old := new(bytes.Buffer)
		if _, err := io.Copy(old, s2.NewReader(bytes.NewReader(enc.Bytes()))); err != nil {
			t.Fatalf("size %d: stock reader: %v", n, err)
		}
		if !bytes.Equal(old.Bytes(), src) {
			t.Fatalf("size %d: stock s2 reader could not read our output", n)
		}

		// Stock s2 writer -> our reader (our bytes reading an older kine's).
		legacy := new(bytes.Buffer)
		w := s2.NewWriter(legacy)
		if err := w.EncodeBuffer(src); err != nil {
			t.Fatalf("size %d: stock writer: %v", n, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("size %d: stock writer close: %v", n, err)
		}
		got := new(bytes.Buffer)
		if err := vc.Decode(bytes.NewReader(legacy.Bytes()), got); err != nil {
			t.Fatalf("size %d: decode legacy: %v", n, err)
		}
		if !bytes.Equal(got.Bytes(), src) {
			t.Fatalf("size %d: could not read stock s2 writer output", n)
		}
	}
}

// TestValueCodecConcurrent exercises the codec's pooled encoders and decoders
// under contention, where a leaked or incompletely reset instance corrupts data.
func TestValueCodecConcurrent(t *testing.T) {
	vc := &valueCodec{}

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			src := bytes.Repeat([]byte{byte(g)}, 1000+g*97)
			for i := 0; i < 100; i++ {
				enc := new(bytes.Buffer)
				if err := vc.Encode(src, enc); err != nil {
					t.Errorf("encode: %v", err)
					return
				}
				dec := new(bytes.Buffer)
				if err := vc.Decode(bytes.NewReader(enc.Bytes()), dec); err != nil {
					t.Errorf("decode: %v", err)
					return
				}
				if !bytes.Equal(dec.Bytes(), src) {
					t.Errorf("goroutine %d iteration %d: corrupted round trip", g, i)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
