// Package wal implements replog.Store on an append-only file, so that a
// replica running in its own process keeps its promise, its accepted values
// and its chosen entries across a restart.
//
// Every Save call appends one record and calls fsync before it returns. A
// record is an 8-byte header (payload length and CRC-32C of the payload,
// both big-endian) followed by a JSON payload. Open scans the file and
// truncates a torn tail: an incomplete last record, or a last record whose
// checksum does not match, as a crash in the middle of a write leaves. A bad
// record that is followed by more data is corruption, not a torn write, and
// Open refuses the file rather than silently dropping what follows it.
//
// The file is never compacted: it grows by one record per Save call, and
// Load replays all of them. That is adequate for a demonstration cluster,
// not for a long-running one.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/oguzhanozfe/paxos-arena/internal/jsonx"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

const (
	headerBytes = 8
	// maxRecordBytes bounds one payload. The largest record holds one
	// accepted or chosen value of replog.MaxValueBytes, base64-encoded.
	maxRecordBytes = 4*replog.MaxValueBytes + 1<<16
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned, wrapped with the offset, by Open for a file whose
// damage is not confined to its last record.
var ErrCorrupt = errors.New("wal: file is corrupt")

// Membership is the identity a log file belongs to: the replica that wrote
// it and the fixed membership of its cluster.
type Membership struct {
	// Self is the replica.
	Self paxos.NodeID `json:"self"`
	// Peers lists every replica including Self, in ascending order.
	Peers []paxos.NodeID `json:"peers"`
}

// record is one payload. Exactly one field is set.
type record struct {
	Membership *Membership   `json:"membership,omitempty"`
	Promised   *paxos.Ballot `json:"promised,omitempty"`
	MaxRound   *uint64       `json:"max_round,omitempty"`
	Accepted   *paxos.PValue `json:"accepted,omitempty"`
	Chosen     *replog.Entry `json:"chosen,omitempty"`
}

func (r record) fields() int {
	n := 0
	for _, set := range []bool{r.Membership != nil, r.Promised != nil, r.MaxRound != nil, r.Accepted != nil, r.Chosen != nil} {
		if set {
			n++
		}
	}
	return n
}

// File is a replog.Store backed by one append-only file. Its methods are
// safe for concurrent use, although a replog.Node calls them from one
// goroutine.
type File struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	size   int64 // length of the valid prefix; the next record goes here
	member *Membership
	err    error // first write error; every later Save returns it
}

var _ replog.Store = (*File)(nil)

// Open opens the log file at path, creating it (and syncing its directory)
// when it does not exist, and truncates a torn tail.
func Open(path string) (*File, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, err
		}
	}
	w := &File{f: f, path: path}
	recs, valid, err := w.scan()
	if err != nil {
		f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat: %w", err)
	}
	if valid < info.Size() {
		if err := f.Truncate(valid); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: truncate torn tail at %d: %w", valid, err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: sync after truncation: %w", err)
		}
	}
	w.size = valid
	for i, r := range recs {
		if r.Membership != nil {
			if i != 0 {
				f.Close()
				return nil, fmt.Errorf("%w: membership record at position %d", ErrCorrupt, i+1)
			}
			w.member = r.Membership
		}
	}
	return w, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: sync directory: %w", err)
	}
	return nil
}

// Path returns the file's path.
func (w *File) Path() string { return w.path }

// scan reads the whole file and returns its records and the length of the
// valid prefix. A torn tail ends the prefix; damage followed by further data
// is ErrCorrupt.
func (w *File) scan() ([]record, int64, error) {
	info, err := w.f.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("wal: stat: %w", err)
	}
	b := make([]byte, info.Size())
	if len(b) > 0 {
		if _, err := w.f.ReadAt(b, 0); err != nil {
			return nil, 0, fmt.Errorf("wal: read: %w", err)
		}
	}
	var recs []record
	off := 0
	for off < len(b) {
		rest := b[off:]
		if len(rest) < headerBytes {
			return recs, int64(off), nil // torn header
		}
		n := int(binary.BigEndian.Uint32(rest[0:4]))
		sum := binary.BigEndian.Uint32(rest[4:8])
		if n == 0 {
			if allZero(rest) {
				return recs, int64(off), nil // zero-filled tail
			}
			return nil, 0, fmt.Errorf("%w: empty record at offset %d", ErrCorrupt, off)
		}
		if n > maxRecordBytes || headerBytes+n > len(rest) {
			return recs, int64(off), nil // torn payload
		}
		payload := rest[headerBytes : headerBytes+n]
		last := headerBytes+n == len(rest)
		var r record
		if crc32.Checksum(payload, castagnoli) != sum {
			if last {
				return recs, int64(off), nil
			}
			return nil, 0, fmt.Errorf("%w: checksum mismatch at offset %d", ErrCorrupt, off)
		}
		if err := jsonx.DecodeStrict(payload, &r); err != nil || r.fields() != 1 {
			// The checksum matched, so this is not a torn write: the record
			// was written this way.
			return nil, 0, fmt.Errorf("%w: undecodable record at offset %d", ErrCorrupt, off)
		}
		recs = append(recs, r)
		off += headerBytes + n
	}
	return recs, int64(off), nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Membership returns the membership recorded in the file, or ok=false when
// none is.
func (w *File) Membership() (Membership, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.member == nil {
		return Membership{}, false
	}
	return Membership{Self: w.member.Self, Peers: slices.Clone(w.member.Peers)}, true
}

// Bind ties the file to replica self in a cluster of peers. On an empty file
// it records the membership. On a file that has one it returns an error
// unless self and the set of peers are the same, so that a replica never
// starts from another replica's log or under a different membership. A file
// with records but no membership is refused too.
func (w *File) Bind(self paxos.NodeID, peers []paxos.NodeID) error {
	want := Membership{Self: self, Peers: slices.Clone(peers)}
	sort.Slice(want.Peers, func(i, j int) bool { return want.Peers[i] < want.Peers[j] })
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.member != nil {
		if w.member.Self != want.Self || !slices.Equal(w.member.Peers, want.Peers) {
			return fmt.Errorf("wal: %s belongs to node %d of %v, not node %d of %v",
				w.path, w.member.Self, w.member.Peers, want.Self, want.Peers)
		}
		return nil
	}
	if w.size > 0 {
		return fmt.Errorf("wal: %s holds log records but no membership", w.path)
	}
	if err := w.appendLocked(record{Membership: &want}); err != nil {
		return err
	}
	w.member = &want
	return nil
}

// Load implements replog.Store: it replays every record, the last one per
// field or slot winning, and returns Accepted and Chosen in slot order.
func (w *File) Load() (replog.Durable, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return replog.Durable{}, errors.New("wal: file is closed")
	}
	if w.err != nil {
		return replog.Durable{}, fmt.Errorf("wal: a write failed; reopen the file: %w", w.err)
	}
	recs, valid, err := w.scan()
	if err != nil {
		return replog.Durable{}, err
	}
	if valid != w.size {
		return replog.Durable{}, fmt.Errorf("%w: valid prefix is %d bytes, %d were written", ErrCorrupt, valid, w.size)
	}
	var d replog.Durable
	accepted := make(map[paxos.Slot]paxos.PValue)
	chosen := make(map[paxos.Slot]replog.Entry)
	for _, r := range recs {
		switch {
		case r.Promised != nil:
			d.Promised = *r.Promised
		case r.MaxRound != nil:
			d.MaxRound = *r.MaxRound
		case r.Accepted != nil:
			accepted[r.Accepted.Slot] = *r.Accepted
		case r.Chosen != nil:
			chosen[r.Chosen.Slot] = *r.Chosen
		}
	}
	d.Accepted = make([]paxos.PValue, 0, len(accepted))
	for _, pv := range accepted {
		d.Accepted = append(d.Accepted, pv)
	}
	sort.Slice(d.Accepted, func(i, j int) bool { return d.Accepted[i].Slot < d.Accepted[j].Slot })
	d.Chosen = make([]replog.Entry, 0, len(chosen))
	for _, e := range chosen {
		d.Chosen = append(d.Chosen, e)
	}
	sort.Slice(d.Chosen, func(i, j int) bool { return d.Chosen[i].Slot < d.Chosen[j].Slot })
	return d, nil
}

// SavePromised implements replog.Store.
func (w *File) SavePromised(b paxos.Ballot) error { return w.append(record{Promised: &b}) }

// SaveMaxRound implements replog.Store.
func (w *File) SaveMaxRound(r uint64) error { return w.append(record{MaxRound: &r}) }

// SaveAccepted implements replog.Store.
func (w *File) SaveAccepted(pv paxos.PValue) error { return w.append(record{Accepted: &pv}) }

// SaveChosen implements replog.Store.
func (w *File) SaveChosen(e replog.Entry) error { return w.append(record{Chosen: &e}) }

func (w *File) append(r record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(r)
}

// appendLocked writes one record at the end of the valid prefix and syncs.
// A failed write or sync poisons the file: the partial record is cut off if
// possible, and every later Save returns the same error, because the node
// that called it has stopped and must be rebuilt from Open and Load.
func (w *File) appendLocked(r record) error {
	if w.err != nil {
		return w.err
	}
	if w.f == nil {
		return errors.New("wal: file is closed")
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("wal: encode record: %w", err)
	}
	if len(payload) > maxRecordBytes {
		return fmt.Errorf("wal: record of %d bytes exceeds the limit %d", len(payload), maxRecordBytes)
	}
	buf := make([]byte, headerBytes+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(payload, castagnoli))
	copy(buf[headerBytes:], payload)
	if _, err := w.f.WriteAt(buf, w.size); err != nil {
		w.err = fmt.Errorf("wal: write: %w", err)
		w.f.Truncate(w.size)
		return w.err
	}
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: sync: %w", err)
		return w.err
	}
	w.size += int64(len(buf))
	return nil
}

// Close closes the file. Every later call returns an error.
func (w *File) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
