package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reearth/ygo/crdt"
	"github.com/reearth/ygo/persistence"
	"github.com/reearth/ygo/persistence/sqlite"
)

// backupBytes runs Backup against s and returns the artifact bytes (an
// in-memory equivalent of BackupFile) so the parsing/corruption tests do not
// need on-disk plumbing.
func backupBytes(t *testing.T, ctx context.Context, s *sqlite.Store) ([]byte, sqlite.BackupInfo) {
	t.Helper()
	var buf bytes.Buffer
	info, err := sqlite.Backup(ctx, s, &buf)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	// 28 header + 10 END frame + 40 END body + 8 footer is the empty minimum.
	if buf.Len() < 28+10+40+8 {
		t.Fatalf("artifact suspiciously short: %d bytes", buf.Len())
	}
	return buf.Bytes(), info
}

// expectedRoom captures what the comparison helper needs beyond what the store
// API can enumerate: the legacy name-keyed snapshots have no list API.
type expectedRoom struct {
	namedNames []string
}

// buildPopulatedStore creates a store at path with three rooms covering every
// data class the backup must carry:
//   - "alpha"/"beta": two incremental updates each, two named snapshots, three
//     id-keyed snapshots (a repeated label, proving labels are not unique)
//   - "snaps-only": no updates at all, one named and one id-keyed snapshot
func buildPopulatedStore(t *testing.T, path string) (*sqlite.Store, map[string]expectedRoom) {
	t.Helper()
	s := mustOpen(t, path)
	ctx := context.Background()

	for i, room := range []string{"alpha", "beta"} {
		doc := crdt.New(crdt.WithClientID(crdt.ClientID(10 + i)))
		txt := doc.GetText("t")
		doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, "a", nil) })
		u1 := crdt.EncodeStateAsUpdateV1(doc, nil)
		sv := doc.StateVector()
		doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 1, "b", nil) })
		u2 := crdt.EncodeStateAsUpdateV1(doc, sv)
		if v, err := s.AppendUpdate(ctx, room, u1); err != nil || v != 1 {
			t.Fatalf("%s append1: v=%d err=%v", room, v, err)
		}
		if v, err := s.AppendUpdate(ctx, room, u2); err != nil || v != 2 {
			t.Fatalf("%s append2: v=%d err=%v", room, v, err)
		}
		for _, name := range []string{"first", "second"} {
			if _, err := s.CaptureSnapshot(ctx, room, name, []byte("named-state-"+room+"-"+name)); err != nil {
				t.Fatalf("%s CaptureSnapshot(%s): %v", room, name, err)
			}
		}
		if _, err := s.SaveSnapshot(ctx, room, "auto", []byte("vsnap-"+room+"-1")); err != nil {
			t.Fatalf("%s SaveSnapshot: %v", room, err)
		}
		if _, err := s.SaveSnapshot(ctx, room, "auto", []byte("vsnap-"+room+"-2")); err != nil {
			t.Fatalf("%s SaveSnapshot 2: %v", room, err)
		}
		if _, err := s.SaveSnapshot(ctx, room, "release", []byte("vsnap-"+room+"-r")); err != nil {
			t.Fatalf("%s SaveSnapshot release: %v", room, err)
		}
	}

	if _, err := s.CaptureSnapshot(ctx, "snaps-only", "legacy", []byte("only-named")); err != nil {
		t.Fatalf("snaps-only CaptureSnapshot: %v", err)
	}
	if _, err := s.SaveSnapshot(ctx, "snaps-only", "tag", []byte("only-versioned")); err != nil {
		t.Fatalf("snaps-only SaveSnapshot: %v", err)
	}

	want := map[string]expectedRoom{
		"alpha":      {namedNames: []string{"first", "second"}},
		"beta":       {namedNames: []string{"first", "second"}},
		"snaps-only": {namedNames: []string{"legacy"}},
	}
	return s, want
}

// assertStoresEquivalent compares two stores through the PUBLIC persistence
// API: room set, clamped heads, per-version metadata and payloads,
// materialization, legacy named snapshots, and enumerable snapshots incl.
// timestamps and ids. This is the contract "restore yields the same store".
func assertStoresEquivalent(t *testing.T, ctx context.Context, got, want *sqlite.Store, expect map[string]expectedRoom) {
	t.Helper()
	gotRooms, err := got.ListRooms(ctx)
	if err != nil {
		t.Fatalf("got ListRooms: %v", err)
	}
	wantRooms, err := want.ListRooms(ctx)
	if err != nil {
		t.Fatalf("want ListRooms: %v", err)
	}
	sort.Strings(gotRooms)
	sort.Strings(wantRooms)
	if strings.Join(gotRooms, ",") != strings.Join(wantRooms, ",") {
		t.Fatalf("rooms = %v, want %v", gotRooms, wantRooms)
	}

	for _, room := range wantRooms {
		gl, err := got.Load(ctx, room)
		if err != nil {
			t.Fatalf("%s got Load: %v", room, err)
		}
		wl, err := want.Load(ctx, room)
		if err != nil {
			t.Fatalf("%s want Load: %v", room, err)
		}
		if gl.Version != wl.Version {
			t.Fatalf("%s head version = %d, want %d", room, gl.Version, wl.Version)
		}
		if !bytes.Equal(gl.Update, wl.Update) {
			t.Fatalf("%s head update: %d bytes, want %d bytes (content differs)",
				room, len(gl.Update), len(wl.Update))
		}

		gv, err := got.ListVersions(ctx, room)
		if err != nil {
			t.Fatalf("%s got ListVersions: %v", room, err)
		}
		wv, err := want.ListVersions(ctx, room)
		if err != nil {
			t.Fatalf("%s want ListVersions: %v", room, err)
		}
		if len(gv) != len(wv) {
			t.Fatalf("%s versions: got %d entries, want %d", room, len(gv), len(wv))
		}
		for i := range wv {
			if gv[i].Version != wv[i].Version ||
				gv[i].UpdatedAt.UnixNano() != wv[i].UpdatedAt.UnixNano() {
				t.Fatalf("%s version meta[%d] = %+v, want %+v", room, i, gv[i], wv[i])
			}
			gb, gm, gok, err := got.GetUpdate(ctx, room, wv[i].Version)
			if err != nil || !gok {
				t.Fatalf("%s GetUpdate(%d): ok=%v err=%v", room, wv[i].Version, gok, err)
			}
			wb, wm, _, err := want.GetUpdate(ctx, room, wv[i].Version)
			if err != nil {
				t.Fatalf("%s want GetUpdate(%d): %v", room, wv[i].Version, err)
			}
			if !bytes.Equal(gb, wb) || gm.UpdatedAt.UnixNano() != wm.UpdatedAt.UnixNano() {
				t.Fatalf("%s GetUpdate(%d) payload/meta mismatch", room, wv[i].Version)
			}
		}
		if gl.Version > 0 {
			gm, err := got.MaterializeAt(ctx, room, gl.Version)
			if err != nil {
				t.Fatalf("%s got MaterializeAt: %v", room, err)
			}
			wm, err := want.MaterializeAt(ctx, room, wl.Version)
			if err != nil {
				t.Fatalf("%s want MaterializeAt: %v", room, err)
			}
			if !bytes.Equal(gm, wm) || !bytes.Equal(gm, gl.Update) {
				t.Fatalf("%s MaterializeAt(head) mismatch", room)
			}
		}

		for _, name := range expect[room].namedNames {
			gs, gv2, gok, err := got.RestoreSnapshot(ctx, room, name)
			if err != nil || !gok {
				t.Fatalf("%s got RestoreSnapshot(%s): ok=%v err=%v", room, name, gok, err)
			}
			ws, wv2, wok, err := want.RestoreSnapshot(ctx, room, name)
			if err != nil || !wok {
				t.Fatalf("%s want RestoreSnapshot(%s): ok=%v err=%v", room, name, wok, err)
			}
			if gv2 != wv2 || !bytes.Equal(gs, ws) {
				t.Fatalf("%s named snapshot %s mismatch: v %d/%d bytes %d/%d",
					room, name, gv2, wv2, len(gs), len(ws))
			}
		}

		gs, err := got.ListSnapshots(ctx, room)
		if err != nil {
			t.Fatalf("%s got ListSnapshots: %v", room, err)
		}
		ws, err := want.ListSnapshots(ctx, room)
		if err != nil {
			t.Fatalf("%s want ListSnapshots: %v", room, err)
		}
		if len(gs) != len(ws) {
			t.Fatalf("%s snapshots: got %d, want %d", room, len(gs), len(ws))
		}
		for i := range ws {
			if gs[i].ID != ws[i].ID || gs[i].Label != ws[i].Label ||
				gs[i].CreatedAt.UnixNano() != ws[i].CreatedAt.UnixNano() ||
				gs[i].Size != ws[i].Size {
				t.Fatalf("%s snapshot meta[%d] = %+v, want %+v", room, i, gs[i], ws[i])
			}
			gb, err := got.GetSnapshotState(ctx, room, ws[i].ID)
			if err != nil {
				t.Fatalf("%s GetSnapshotState(%d): %v", room, ws[i].ID, err)
			}
			wb, err := want.GetSnapshotState(ctx, room, ws[i].ID)
			if err != nil {
				t.Fatalf("%s want GetSnapshotState(%d): %v", room, ws[i].ID, err)
			}
			if !bytes.Equal(gb, wb) {
				t.Fatalf("%s snapshot %d state differs", room, ws[i].ID)
			}
		}
	}
}

// mustReopen opens an existing database (failing on error) for comparison.
func mustReopen(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen %q: %v", path, err)
	}
	return s
}

func TestBackupRestore_FullRoundTrip(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "src.db")
	src, want := buildPopulatedStore(t, srcPath)
	artifact, info := backupBytes(t, ctx, src)
	if info.FormatVersion != 1 {
		t.Fatalf("FormatVersion = %d, want 1", info.FormatVersion)
	}
	if info.Rooms != 3 {
		t.Fatalf("Rooms = %d, want 3", info.Rooms)
	}
	if info.CreatedAt.IsZero() {
		t.Fatal("CreatedAt not set")
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	dstPath := filepath.Join(t.TempDir(), "dst.db")
	rinfo, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dstPath, false)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if rinfo.Rooms != 3 {
		t.Fatalf("Restore info rooms = %d, want 3", rinfo.Rooms)
	}

	// Comparison runs through fresh handles on both sides, modeling a real
	// shutdown of the source and a real server start on the restored DB.
	dst := mustReopen(t, dstPath)
	defer dst.Close()
	srcReopened := mustReopen(t, srcPath)
	defer srcReopened.Close()
	assertStoresEquivalent(t, ctx, dst, srcReopened, want)
}

func TestBackupRestore_FileAPI(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	src, want := buildPopulatedStore(t, srcPath)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	backupPath := filepath.Join(dir, "backup.ygo")
	info, err := sqlite.BackupFile(ctx, srcPath, backupPath, false)
	if err != nil {
		t.Fatalf("BackupFile: %v", err)
	}
	if info.Rooms != 3 {
		t.Fatalf("BackupFile rooms = %d, want 3", info.Rooms)
	}
	data, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if !bytes.HasPrefix(data, []byte("YGOBKUP!")) {
		t.Fatalf("backup missing magic header")
	}
	if bytes.HasPrefix(data, []byte("SQLite format 3")) {
		t.Fatalf("backup is a raw SQLite file, not a portable artifact")
	}
	if entries := tmpEntries(t, dir); len(entries) != 0 {
		t.Fatalf("temp files left in backup dir: %v", entries)
	}

	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := sqlite.RestoreFile(ctx, backupPath, dst, false); err != nil {
		t.Fatalf("RestoreFile: %v", err)
	}
	got := mustOpen(t, dst)
	defer got.Close()
	wantStore := mustReopen(t, srcPath)
	defer wantStore.Close()
	assertStoresEquivalent(t, ctx, got, wantStore, want)
	if entries := tmpEntries(t, filepath.Dir(dst)); len(entries) != 0 {
		t.Fatalf("temp files left in restore dir: %v", entries)
	}
}

// tmpEntries returns backup/restore temp debris in dir (our own dot-prefixed
// .tmp names). Both operations must clean these up on every path.
func tmpEntries(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".ygo*-*.tmp"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for i, m := range matches {
		matches[i] = filepath.Base(m)
	}
	return matches
}

func TestBackup_EmptyStore(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "empty.db"))
	artifact, info := backupBytes(t, ctx, s)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if info.Rooms != 0 {
		t.Fatalf("empty store rooms = %d, want 0", info.Rooms)
	}
	dst := filepath.Join(t.TempDir(), "restored-empty.db")
	rinfo, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, false)
	if err != nil {
		t.Fatalf("restore empty artifact: %v", err)
	}
	if rinfo.Rooms != 0 {
		t.Fatalf("restored rooms = %d, want 0", rinfo.Rooms)
	}
	got := mustOpen(t, dst)
	defer got.Close()
	rooms, err := got.ListRooms(ctx)
	if err != nil {
		t.Fatalf("ListRooms: %v", err)
	}
	if len(rooms) != 0 {
		t.Fatalf("restored empty store has rooms %v", rooms)
	}
}

// A backup taken while writers append continuously must capture a single
// consistent point: per room the visible versions are the exact dense prefix
// 1..k, and folding them yields a coherent document (text length equals the
// number of versions). It must never mix two moments (gaps or holes).
func TestBackup_ConcurrentWriters_SinglePoint(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "busy.db"))
	defer s.Close()

	const rooms = 3
	const writersPerRoom = 4
	const updatesPerWriter = 10

	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, rooms*writersPerRoom)
	for r := range rooms {
		room := fmt.Sprintf("room-%d", r)
		for w := range writersPerRoom {
			wg.Add(1)
			go func(room string, client int) {
				defer wg.Done()
				// Distinct client ids keep every update valid; characters are
				// drawn from a per-room-unique space so the merged text length
				// is a meaningful coherence check.
				doc := crdt.New(crdt.WithClientID(crdt.ClientID(1000 + client)))
				txt := doc.GetText("t")
				for i := range updatesPerWriter {
					if stop.Load() {
						return
					}
					ch := byte('A' + ((client*updatesPerWriter + i) % 26))
					doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, string(ch), nil) })
					upd := crdt.EncodeStateAsUpdateV1(doc, nil)
					if _, err := s.AppendUpdate(ctx, room, upd); err != nil {
						errCh <- fmt.Errorf("%s append: %w", room, err)
						return
					}
				}
			}(room, r*writersPerRoom+w)
		}
	}

	// Wait until at least one commit is visible before snapshotting: a backup
	// taken before the writers start would legitimately be an empty store.
	deadline := time.Now().Add(5 * time.Second)
	for {
		vers, err := s.ListVersions(ctx, "room-0")
		if err != nil {
			t.Fatalf("ListVersions poll: %v", err)
		}
		if len(vers) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writers never committed before the backup window")
		}
		time.Sleep(time.Millisecond)
	}

	// Take several backups mid-flight; EVERY artifact must be internally
	// consistent even though the source keeps changing.
	var artifacts [][]byte
	for range 5 {
		var buf bytes.Buffer
		if _, err := sqlite.Backup(ctx, s, &buf); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("concurrent Backup: %v", err)
		}
		artifacts = append(artifacts, bytes.Clone(buf.Bytes()))
		time.Sleep(2 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("writer: %v", err)
	}

	seenRooms := map[string]bool{}
	for i, artifact := range artifacts {
		dst := filepath.Join(t.TempDir(), fmt.Sprintf("busy-%d.db", i))
		if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, false); err != nil {
			t.Fatalf("restore artifact %d: %v", i, err)
		}
		r := mustReopen(t, dst)
		gotRooms, err := r.ListRooms(ctx)
		if err != nil {
			t.Fatalf("artifact %d ListRooms: %v", i, err)
		}
		// We only waited for room-0 before the first capture, so early
		// artifacts may legitimately contain a subset of rooms; every room
		// present must itself be internally consistent.
		if len(gotRooms) == 0 {
			t.Fatalf("artifact %d captured no rooms", i)
		}
		for _, room := range gotRooms {
			seenRooms[room] = true
		}
		for _, room := range gotRooms {
			vers, err := r.ListVersions(ctx, room)
			if err != nil {
				t.Fatalf("artifact %d %s ListVersions: %v", i, room, err)
			}
			if len(vers) == 0 {
				t.Fatalf("artifact %d %s captured a room with zero visible versions", i, room)
			}
			// Newest-first: must be exactly the dense prefix k..1 — a gap or
			// duplicate hole would mean the backup mixed two moments.
			for idx, meta := range vers {
				wantV := persistence.Version(len(vers) - idx)
				if meta.Version != wantV {
					t.Fatalf("artifact %d %s mixed snapshot: got v%d at index %d, want v%d (versions %v)",
						i, room, meta.Version, idx, wantV, vers)
				}
				_, _, ok, err := r.GetUpdate(ctx, room, meta.Version)
				if err != nil || !ok {
					t.Fatalf("artifact %d %s GetUpdate(%d): ok=%v err=%v", i, room, meta.Version, ok, err)
				}
			}
			lr, err := r.Load(ctx, room)
			if err != nil {
				t.Fatalf("artifact %d %s Load: %v", i, room, err)
			}
			if int(lr.Version) != len(vers) {
				t.Fatalf("artifact %d %s head %d != versions %d", i, room, lr.Version, len(vers))
			}
			d := crdt.New()
			if err := crdt.ApplyUpdateV1(d, lr.Update, nil); err != nil {
				t.Fatalf("artifact %d %s apply head: %v", i, room, err)
			}
			txtLen := len(d.GetText("t").ToString())
			if txtLen != len(vers) {
				t.Fatalf("artifact %d %s materialized text len %d != version count %d (mixed snapshot)",
					i, room, txtLen, len(vers))
			}
		}
		r.Close()
	}
	// Across the capture window writers committed to every room, so the union
	// of the single-point snapshots must cover them all even if early
	// artifacts missed the rooms that had not yet received their first commit.
	if len(seenRooms) != rooms {
		t.Fatalf("union of captured rooms = %v, want all %d rooms", seenRooms, rooms)
	}
}

// An interrupted prune's checkpoint must be carried by the backup and keep the
// visible head clamped after restore; appending after restore then recovers
// cleanly (head advances from the clamped version, never from the hidden
// future rows that were excluded).
func TestBackupRestore_MidPruneCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "prune.db"))

	doc := crdt.New(crdt.WithClientID(7))
	txt := doc.GetText("t")
	var upds [][]byte
	for i := range 4 {
		sv := doc.StateVector()
		doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, i, "abcd"[i:i+1], nil) })
		upds = append(upds, crdt.EncodeStateAsUpdateV1(doc, sv))
	}
	for i, u := range upds {
		if v, err := s.AppendUpdate(ctx, "doc", u); err != nil || v != persistence.Version(i+1) {
			t.Fatalf("append %d: v=%d err=%v", i+1, v, err)
		}
	}
	at2, err := s.MaterializeAt(ctx, "doc", 2)
	if err != nil {
		t.Fatalf("MaterializeAt(2): %v", err)
	}
	at2Doc := crdt.New()
	if err := crdt.ApplyUpdateV1(at2Doc, at2, nil); err != nil {
		t.Fatalf("apply at2: %v", err)
	}
	wantText := at2Doc.GetText("t").ToString()

	// Crash between the checkpoint write and the future-row deletes.
	s.SetCrashAfterCheckpoint(func() bool { return true })
	if err := s.PruneAfter(ctx, "doc", 2, at2); err != nil {
		t.Fatalf("PruneAfter: %v", err)
	}
	s.SetCrashAfterCheckpoint(nil)

	lr, err := s.Load(ctx, "doc")
	if err != nil || lr.Version != 2 {
		t.Fatalf("source clamped head = %d err=%v, want 2", lr.Version, err)
	}

	var buf bytes.Buffer
	if _, err := sqlite.Backup(ctx, s, &buf); err != nil {
		t.Fatalf("Backup mid-prune: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "prune-restored.db")
	if _, err := sqlite.Restore(ctx, bytes.NewReader(buf.Bytes()), dst, false); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	r := mustOpen(t, dst)
	defer r.Close()

	lr2, err := r.Load(ctx, "doc")
	if err != nil {
		t.Fatalf("restored Load: %v", err)
	}
	if lr2.Version != 2 {
		t.Fatalf("restored clamped head = %d, want 2", lr2.Version)
	}
	d := crdt.New()
	if err := crdt.ApplyUpdateV1(d, lr2.Update, nil); err != nil {
		t.Fatalf("apply restored head: %v", err)
	}
	if got := d.GetText("t").ToString(); got != wantText {
		t.Fatalf("restored clamped text = %q, want %q", got, wantText)
	}
	vers, err := r.ListVersions(ctx, "doc")
	if err != nil || len(vers) != 2 {
		t.Fatalf("restored visible versions = %v err=%v, want 2 entries", vers, err)
	}
	// Materializing above the ceiling clamps exactly like the source.
	m4, err := r.MaterializeAt(ctx, "doc", 4)
	if err != nil {
		t.Fatalf("MaterializeAt(4): %v", err)
	}
	if !bytes.Equal(m4, at2) {
		t.Fatalf("MaterializeAt(4) did not clamp to the v2 checkpoint state")
	}

	// Appending recovers the prune and continues at target+1=3.
	next := crdt.New(crdt.WithClientID(99))
	nxt := next.GetText("t")
	next.Transact(func(txn *crdt.Transaction) { nxt.Insert(txn, 0, "X", nil) })
	nx := crdt.EncodeStateAsUpdateV1(next, nil)
	v, err := r.AppendUpdate(ctx, "doc", nx)
	if err != nil {
		t.Fatalf("post-restore append: %v", err)
	}
	if v != 3 {
		t.Fatalf("post-restore append version = %d, want 3", v)
	}
	lr3, err := r.Load(ctx, "doc")
	if err != nil || lr3.Version != 3 {
		t.Fatalf("post-recovery head = %d err=%v, want 3", lr3.Version, err)
	}
	fd := crdt.New()
	if err := crdt.ApplyUpdateV1(fd, lr3.Update, nil); err != nil {
		t.Fatalf("apply recovered head: %v", err)
	}
	text := fd.GetText("t").ToString()
	if len(text) != 3 || !strings.Contains(text, "X") {
		t.Fatalf("post-recovery text = %q, want 3 chars containing X", text)
	}
}

// Restoring preserves the snapshot_versions AUTOINCREMENT sequence: a new
// snapshot after restore gets an id above every restored one.
func TestBackupRestore_VersionedIDsContinue(t *testing.T) {
	ctx := context.Background()
	s, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	artifact, _ := backupBytes(t, ctx, s)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "ids.db")
	if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, false); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	r := mustOpen(t, dst)
	defer r.Close()
	snaps, err := r.ListSnapshots(ctx, "alpha")
	if err != nil || len(snaps) != 3 {
		t.Fatalf("ListSnapshots: %v %v", snaps, err)
	}
	maxID := snaps[0].ID // newest-first
	id, err := r.SaveSnapshot(ctx, "alpha", "after", []byte("post-restore"))
	if err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if id <= maxID {
		t.Fatalf("new snapshot id %d not above restored max %d (AUTOINCREMENT not preserved)", id, maxID)
	}
}

func TestRestore_TargetOccupied(t *testing.T) {
	ctx := context.Background()
	src, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	artifact, _ := backupBytes(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dir := t.TempDir()
	dst := filepath.Join(dir, "occupied.db")
	existing := mustOpen(t, dst)
	// Sentinel content proving the existing database survives every attempt.
	sentinel := updateWithText(t, 4242, "sentinel")
	if _, err := existing.AppendUpdate(ctx, "keepme", sentinel); err != nil {
		t.Fatalf("sentinel append: %v", err)
	}
	if err := existing.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 1. Without force: refused, original fully intact.
	if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, false); !errors.Is(err, sqlite.ErrBackupTargetExists) {
		t.Fatalf("Restore occupied = %v, want ErrBackupTargetExists", err)
	}
	check := mustOpen(t, dst)
	lr, err := check.Load(ctx, "keepme")
	if err != nil || lr.Version != 1 {
		t.Fatalf("original DB after refused restore: v=%d err=%v", lr.Version, err)
	}
	d := crdt.New()
	if err := crdt.ApplyUpdateV1(d, lr.Update, nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := d.GetText("t").ToString(); got != "sentinel" {
		t.Fatalf("original content = %q, want sentinel", got)
	}
	check.Close()

	// 2. A CORRUPT artifact with force still must not touch the original:
	// validation precedes installation.
	bad := bytes.Clone(artifact)
	bad[len(bad)/2] ^= 0xff
	if _, err := sqlite.Restore(ctx, bytes.NewReader(bad), dst, true); err == nil {
		t.Fatal("corrupt restore with force succeeded")
	}
	check = mustOpen(t, dst)
	lr, _ = check.Load(ctx, "keepme")
	if lr.Version != 1 {
		t.Fatalf("original DB replaced despite corrupt artifact: v=%d", lr.Version)
	}
	check.Close()

	// 3. With force and a valid artifact the target is replaced.
	if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, true); err != nil {
		t.Fatalf("forced restore: %v", err)
	}
	replaced := mustOpen(t, dst)
	defer replaced.Close()
	rooms, err := replaced.ListRooms(ctx)
	if err != nil {
		t.Fatalf("ListRooms: %v", err)
	}
	sort.Strings(rooms)
	if strings.Join(rooms, ",") != "alpha,beta,snaps-only" {
		t.Fatalf("forced restore rooms = %v", rooms)
	}
	if _, _, ok, _ := replaced.GetUpdate(ctx, "keepme", 1); ok {
		t.Fatal("old database content survived a forced restore")
	}
	if entries := tmpEntries(t, dir); len(entries) != 0 {
		t.Fatalf("temp debris after restore: %v", entries)
	}
}

func TestBackupFile_ExistingOutputProtected(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "src.db")
	src, _ := buildPopulatedStore(t, srcPath)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "existing.ygo")
	sentinel := []byte("PREVIOUS BACKUP")
	if err := os.WriteFile(out, sentinel, 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	if _, err := sqlite.BackupFile(ctx, srcPath, out, false); !errors.Is(err, sqlite.ErrBackupFileExists) {
		t.Fatalf("BackupFile existing = %v, want ErrBackupFileExists", err)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, sentinel) {
		t.Fatalf("existing backup modified: %q err=%v", got, err)
	}

	// Even with force, a canceled run preserves the old artifact: the rename
	// is the first and only mutation at the destination path.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := sqlite.BackupFile(cctx, srcPath, out, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled BackupFile = %v, want context.Canceled", err)
	}
	got, err = os.ReadFile(out)
	if err != nil || !bytes.Equal(got, sentinel) {
		t.Fatalf("existing backup modified by failed run: %q err=%v", got, err)
	}
	if entries := tmpEntries(t, dir); len(entries) != 0 {
		t.Fatalf("temp debris: %v", entries)
	}

	// Forced successful run replaces it with a valid artifact.
	info, err := sqlite.BackupFile(ctx, srcPath, out, true)
	if err != nil {
		t.Fatalf("forced BackupFile: %v", err)
	}
	if info.Rooms != 3 {
		t.Fatalf("rooms = %d", info.Rooms)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if bytes.Equal(data, sentinel) || !bytes.HasPrefix(data, []byte("YGOBKUP!")) {
		t.Fatal("forced backup did not replace the old artifact correctly")
	}
}

func TestRestore_CorruptAndIncompatible(t *testing.T) {
	ctx := context.Background()
	src, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	good, _ := backupBytes(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func([]byte) []byte
		wantErr error
	}{
		{"empty", func([]byte) []byte { return nil }, sqlite.ErrNotABackup},
		{"raw sqlite database", func([]byte) []byte {
			return append([]byte("SQLite format 3\x00"), make([]byte, 100)...)
		}, sqlite.ErrNotABackup},
		{"bad magic", func(b []byte) []byte {
			c := bytes.Clone(b)
			copy(c[0:8], "NOPE!!!!")
			return c
		}, sqlite.ErrNotABackup},
		{"unsupported version", func(b []byte) []byte {
			c := bytes.Clone(b)
			c[8], c[9], c[10], c[11] = 0, 0, 0, 99
			return c
		}, sqlite.ErrUnsupportedBackupVersion},
		{"truncated one byte", func(b []byte) []byte { return b[:len(b)-1] }, sqlite.ErrBackupCorrupt},
		{"truncated footer", func(b []byte) []byte { return b[:len(b)-8] }, sqlite.ErrBackupCorrupt},
		{"truncated end record", func(b []byte) []byte { return b[:len(b)-40] }, sqlite.ErrBackupCorrupt},
		{"truncated mid payload", func(b []byte) []byte { return b[:len(b)/2] }, sqlite.ErrBackupCorrupt},
		{"header only", func(b []byte) []byte { return b[:28] }, sqlite.ErrBackupCorrupt},
		{"flipped payload byte", func(b []byte) []byte {
			c := bytes.Clone(b)
			c[40] ^= 0xff
			return c
		}, sqlite.ErrBackupChecksum},
		{"trailing garbage", func(b []byte) []byte { return append(bytes.Clone(b), 0xAA) }, sqlite.ErrBackupCorrupt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "must-not-exist.db")
			_, err := sqlite.Restore(ctx, bytes.NewReader(tc.mutate(good)), dst, true)
			if err == nil {
				t.Fatalf("restore %s succeeded", tc.name)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("restore %s: %v, want error wrapping %v", tc.name, err, tc.wantErr)
			}
			if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
				t.Fatalf("target created despite %s artifact (stat err=%v)", tc.name, statErr)
			}
			if entries := tmpEntries(t, filepath.Dir(dst)); len(entries) != 0 {
				t.Fatalf("temp debris after %s: %v", tc.name, entries)
			}
		})
	}
}

// A raw SQLite DB or a leftover temp file passed as the backup INPUT must be
// rejected, never interpreted as a valid backup.
func TestRestoreFile_TempAndRawDBRejected(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	rawDB := filepath.Join(dir, "plain.db")
	db := mustOpen(t, rawDB)
	if _, err := db.AppendUpdate(ctx, "r", updateWithText(t, 1, "x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	dst1 := filepath.Join(t.TempDir(), "a.db")
	if _, err := sqlite.RestoreFile(ctx, rawDB, dst1, false); !errors.Is(err, sqlite.ErrNotABackup) {
		t.Fatalf("RestoreFile(raw sqlite db) = %v, want ErrNotABackup", err)
	}
	if _, err := os.Stat(dst1); !os.IsNotExist(err) {
		t.Fatal("target created from raw db input")
	}

	leftover := filepath.Join(dir, ".ygobackup-deadbeef.tmp")
	if err := os.WriteFile(leftover, []byte("partial backup write"), 0o600); err != nil {
		t.Fatalf("write leftover: %v", err)
	}
	dst2 := filepath.Join(t.TempDir(), "b.db")
	if _, err := sqlite.RestoreFile(ctx, leftover, dst2, false); !errors.Is(err, sqlite.ErrNotABackup) {
		t.Fatalf("RestoreFile(leftover temp) = %v, want ErrNotABackup", err)
	}
	if _, err := os.Stat(dst2); !os.IsNotExist(err) {
		t.Fatal("target created from leftover temp input")
	}
}

// Temp debris from an unrelated failed run must not be mistaken for the
// installed database, and a successful restore must not delete unrelated
// files in the target directory.
func TestRestore_UnrelatedTempIgnored(t *testing.T) {
	ctx := context.Background()
	src, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	artifact, _ := backupBytes(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dir := t.TempDir()
	noise := filepath.Join(dir, ".ygorestore-0011223344.tmp")
	if err := os.WriteFile(noise, []byte("abandoned temp db"), 0o600); err != nil {
		t.Fatalf("write noise: %v", err)
	}
	dst := filepath.Join(dir, "real.db")
	if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, false); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := os.Stat(noise); err != nil {
		t.Fatalf("unrelated temp file removed: %v", err)
	}
	got := mustOpen(t, dst)
	rooms, _ := got.ListRooms(ctx)
	if err := got.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(rooms) != 3 {
		t.Fatalf("rooms = %v, want 3 (unrelated temp confused the install?)", rooms)
	}
}

func TestBackupFile_RejectsDatabaseAndSidecars(t *testing.T) {
	ctx := context.Background()
	srcPath := filepath.Join(t.TempDir(), "data.db")
	s, _ := buildPopulatedStore(t, srcPath)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for _, out := range []string{srcPath, srcPath + "-wal", srcPath + "-shm", srcPath + "-journal"} {
		if _, err := sqlite.BackupFile(ctx, srcPath, out, true); err == nil {
			t.Fatalf("BackupFile over %s succeeded", filepath.Base(out))
		}
		if _, err := os.Stat(out); filepath.Base(out) != "data.db" && !os.IsNotExist(err) {
			t.Fatalf("sidecar %s created despite rejection", filepath.Base(out))
		}
	}
}

func TestBackup_InMemoryRejected(t *testing.T) {
	ctx := context.Background()
	if _, err := sqlite.BackupFile(ctx, "", filepath.Join(t.TempDir(), "x.ygo"), false); !errors.Is(err, sqlite.ErrInMemoryNotBackupable) {
		t.Fatalf("BackupFile(\"\") = %v, want ErrInMemoryNotBackupable", err)
	}
	if _, err := sqlite.BackupFile(ctx, ":memory:", filepath.Join(t.TempDir(), "x.ygo"), false); !errors.Is(err, sqlite.ErrInMemoryNotBackupable) {
		t.Fatalf("BackupFile(:memory:) = %v, want ErrInMemoryNotBackupable", err)
	}
}

func TestRestore_BadPaths(t *testing.T) {
	ctx := context.Background()
	src, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	artifact, _ := backupBytes(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, dst := range []string{"", ":memory:"} {
		if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dst, false); err == nil {
			t.Fatalf("Restore(%q) succeeded", dst)
		}
	}
	// Directory target.
	dir := t.TempDir()
	if _, err := sqlite.Restore(ctx, bytes.NewReader(artifact), dir, true); err == nil {
		t.Fatal("Restore into a directory succeeded")
	}
	// Missing backup input.
	if _, err := sqlite.RestoreFile(ctx, filepath.Join(t.TempDir(), "nope.ygo"),
		filepath.Join(t.TempDir(), "x.db"), false); err == nil {
		t.Fatal("RestoreFile missing input succeeded")
	}
}

func TestRestore_CanceledContext_PreservesTarget(t *testing.T) {
	ctx := context.Background()
	src, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	artifact, _ := backupBytes(t, ctx, src)
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()

	dir := t.TempDir()
	dst := filepath.Join(dir, "canceled.db")
	if _, err := sqlite.Restore(cctx, bytes.NewReader(artifact), dst, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Restore = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("target created despite pre-canceled context")
	}
	if entries := tmpEntries(t, dir); len(entries) != 0 {
		t.Fatalf("temp debris after canceled restore: %v", entries)
	}
}

// limitedWriter fails after accepting n bytes, standing in for a full disk or
// a broken pipe at the artifact layer.
type limitedWriter struct {
	left int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.left <= 0 {
		return 0, errors.New("simulated disk full")
	}
	if len(p) > w.left {
		w.left = 0
		return 0, errors.New("simulated disk full")
	}
	w.left -= len(p)
	return len(p), nil
}

func TestBackup_WriterFailure(t *testing.T) {
	ctx := context.Background()
	s, _ := buildPopulatedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer s.Close()
	if _, err := sqlite.Backup(ctx, s, &limitedWriter{left: 50}); err == nil {
		t.Fatal("Backup into a failing writer succeeded")
	}
}

// updateWithText builds a full-state V1 update inserting text for a doc with
// the given client id.
func updateWithText(t *testing.T, client uint64, text string) []byte {
	t.Helper()
	d := crdt.New(crdt.WithClientID(crdt.ClientID(client)))
	txt := d.GetText("t")
	d.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, text, nil) })
	return crdt.EncodeStateAsUpdateV1(d, nil)
}
