package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/reearth/ygo/crdt"
	"github.com/reearth/ygo/persistence"
	"github.com/reearth/ygo/persistence/sqlite"
)

// seedCLIDB creates a SQLite database at dbPath with room "doc" holding two
// updates (text "ab"), one legacy named snapshot, and one id-keyed snapshot.
func seedCLIDB(t *testing.T, dbPath string) {
	t.Helper()
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open %q: %v", dbPath, err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	doc := crdt.New(crdt.WithClientID(1))
	txt := doc.GetText("t")
	doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, "a", nil) })
	u1 := crdt.EncodeStateAsUpdateV1(doc, nil)
	sv := doc.StateVector()
	doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 1, "b", nil) })
	u2 := crdt.EncodeStateAsUpdateV1(doc, sv)
	if _, err := s.AppendUpdate(ctx, "doc", u1); err != nil {
		t.Fatalf("append1: %v", err)
	}
	if _, err := s.AppendUpdate(ctx, "doc", u2); err != nil {
		t.Fatalf("append2: %v", err)
	}
	if _, err := s.CaptureSnapshot(ctx, "doc", "named", []byte("cli-named")); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if _, err := s.SaveSnapshot(ctx, "doc", "auto", []byte("cli-versioned")); err != nil {
		t.Fatalf("save: %v", err)
	}
}

func assertCLIRestoredDB(t *testing.T, dbPath string) {
	t.Helper()
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open restored %q: %v", dbPath, err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	rooms, err := s.ListRooms(ctx)
	if err != nil || len(rooms) != 1 || rooms[0] != "doc" {
		t.Fatalf("restored rooms = %v err=%v, want [doc]", rooms, err)
	}
	lr, err := s.Load(ctx, "doc")
	if err != nil || lr.Version != 2 {
		t.Fatalf("restored head = %d err=%v, want 2", lr.Version, err)
	}
	d := crdt.New()
	if err := crdt.ApplyUpdateV1(d, lr.Update, nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := d.GetText("t").ToString(); got != "ab" {
		t.Fatalf("restored text = %q, want %q", got, "ab")
	}
	at, err := s.MaterializeAt(ctx, "doc", 2)
	if err != nil {
		t.Fatalf("MaterializeAt: %v", err)
	}
	if !bytes.Equal(at, lr.Update) {
		t.Fatal("restored MaterializeAt(2) does not match head")
	}
	named, nv, ok, err := s.RestoreSnapshot(ctx, "doc", "named")
	if err != nil || !ok || nv != 2 || string(named) != "cli-named" {
		t.Fatalf("restored named snapshot: blob=%q v=%d ok=%v err=%v", named, nv, ok, err)
	}
	snaps, err := s.ListSnapshots(ctx, "doc")
	if err != nil || len(snaps) != 1 {
		t.Fatalf("restored snapshots = %v err=%v", snaps, err)
	}
	state, err := s.GetSnapshotState(ctx, "doc", snaps[0].ID)
	if err != nil || string(state) != "cli-versioned" {
		t.Fatalf("restored versioned snapshot = %q err=%v", state, err)
	}
}

// End-to-end CLI: realMain backup then restore, and the restored database
// answers the same room/head/history/snapshot queries as the backup point.
// Neither subcommand may start a listener or Relay — proven by the calls
// returning without ever binding (and without an -addr flag existing).
func TestRealMain_BackupRestore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data.db")
	backupPath := filepath.Join(dir, "data.ygo")
	seedCLIDB(t, dbPath)

	if code := realMain([]string{"backup", "-store", dbPath, "-out", backupPath}); code != 0 {
		t.Fatalf("realMain backup code = %d, want 0", code)
	}
	data, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if !bytes.HasPrefix(data, []byte("YGOBKUP!")) {
		t.Fatal("artifact has no backup magic")
	}
	info, err := os.Stat(backupPath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("artifact not written: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "restored.db")
	if code := realMain([]string{"restore", "-in", backupPath, "-store", dst}); code != 0 {
		t.Fatalf("realMain restore code = %d, want 0", code)
	}
	assertCLIRestoredDB(t, dst)
}

// Restore refuses an occupied target without -force (exit 1) and the original
// database survives; with -force it is replaced.
func TestRealMain_RestoreOccupiedTarget(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	seedCLIDB(t, src)
	backupPath := filepath.Join(dir, "data.ygo")
	if code := realMain([]string{"backup", "-store", src, "-out", backupPath}); code != 0 {
		t.Fatalf("backup code = %d", code)
	}

	dst := filepath.Join(dir, "dst.db")
	seedCLIDB(t, dst)

	if code := realMain([]string{"restore", "-in", backupPath, "-store", dst}); code != 1 {
		t.Fatalf("restore occupied without force code = %d, want 1", code)
	}
	// The original is still a working database with its data intact.
	s, err := sqlite.Open(dst)
	if err != nil {
		t.Fatalf("original DB no longer openable: %v", err)
	}
	lr, _ := s.Load(context.Background(), "doc")
	_ = s.Close()
	if lr.Version != 2 {
		t.Fatalf("original DB damaged by refused restore: head=%d", lr.Version)
	}

	if code := realMain([]string{"restore", "-in", backupPath, "-store", dst, "-force"}); code != 0 {
		t.Fatalf("forced restore code = %d, want 0", code)
	}
	assertCLIRestoredDB(t, dst)
}

// A corrupt artifact exits 1 and never creates the target.
func TestRealMain_RestoreCorrupt(t *testing.T) {
	dir := t.TempDir()
	backupPath := filepath.Join(dir, "bad.ygo")
	if err := os.WriteFile(backupPath, []byte("not a backup at all"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	dst := filepath.Join(dir, "never.db")
	if code := realMain([]string{"restore", "-in", backupPath, "-store", dst}); code != 1 {
		t.Fatalf("restore corrupt code = %d, want 1", code)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("target created despite corrupt input")
	}
}

// Backup usage errors exit 2; -h/--help exits 0 like serve mode.
func TestRealMain_BackupUsage(t *testing.T) {
	if code := realMain([]string{"backup"}); code != 2 {
		t.Fatalf("backup with no flags code = %d, want 2", code)
	}
	if code := realMain([]string{"backup", "-store"}); code != 2 {
		t.Fatalf("backup missing flag value code = %d, want 2", code)
	}
	if code := realMain([]string{"backup", "-out", "/tmp/x"}); code != 2 {
		t.Fatalf("backup missing -store code = %d, want 2", code)
	}
	if code := realMain([]string{"backup", "-store", "/tmp/x", "extra-positional"}); code != 2 {
		t.Fatalf("backup with positional arg code = %d, want 2", code)
	}
	if code := realMain([]string{"backup", "-h"}); code != 0 {
		t.Fatalf("backup -h code = %d, want 0", code)
	}
	// Serve-only flags are not accepted by the subcommand FlagSet.
	if code := realMain([]string{"backup", "-store", "/tmp/x", "-out", "/tmp/y", "-addr", ":1"}); code != 2 {
		t.Fatalf("backup accepted serve-only -addr: code = %d, want 2", code)
	}
	// An explicit but non-durable store is a runtime error (1), not usage:
	// the flag was supplied, but nothing durable can be captured.
	if code := realMain([]string{"backup", "-store", ":memory:", "-out", filepath.Join(t.TempDir(), "x.ygo")}); code != 1 {
		t.Fatalf("backup :memory: code = %d, want 1", code)
	}
	missing := filepath.Join(t.TempDir(), "does-not-exist.db")
	if code := realMain([]string{"backup", "-store", missing, "-out", filepath.Join(t.TempDir(), "x.ygo")}); code != 1 {
		t.Fatalf("backup missing source code = %d, want 1", code)
	}
}

func TestRealMain_RestoreUsage(t *testing.T) {
	if code := realMain([]string{"restore"}); code != 2 {
		t.Fatalf("restore with no flags code = %d, want 2", code)
	}
	if code := realMain([]string{"restore", "-in", "/tmp/x"}); code != 2 {
		t.Fatalf("restore missing -store code = %d, want 2", code)
	}
	if code := realMain([]string{"restore", "-store", "/tmp/x", "extra-positional"}); code != 2 {
		t.Fatalf("restore with positional arg code = %d, want 2", code)
	}
	if code := realMain([]string{"restore", "-h"}); code != 0 {
		t.Fatalf("restore -h code = %d, want 0", code)
	}
}

// An existing artifact is not replaced unless -force, and a failed run leaves
// it byte-identical (exercised end to end through the CLI).
func TestRealMain_BackupExistingOutput(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data.db")
	seedCLIDB(t, dbPath)
	out := filepath.Join(dir, "out.ygo")
	sentinel := []byte("OLD")
	if err := os.WriteFile(out, sentinel, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if code := realMain([]string{"backup", "-store", dbPath, "-out", out}); code != 1 {
		t.Fatalf("overwrite without force code = %d, want 1", code)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, sentinel) {
		t.Fatalf("existing artifact changed: %q err=%v", got, err)
	}
	if code := realMain([]string{"backup", "-store", dbPath, "-out", out, "-force"}); code != 0 {
		t.Fatalf("forced backup code = %d", code)
	}
	got, err = os.ReadFile(out)
	if err != nil || bytes.Equal(got, sentinel) || !bytes.HasPrefix(got, []byte("YGOBKUP!")) {
		t.Fatalf("artifact not replaced by forced backup (len=%d err=%v)", len(got), err)
	}
}

// Subcommand flag parsing directly (defaults and values the CLI relies on).
func TestMaintenanceFlagSets(t *testing.T) {
	var bo backupOpts
	bfs := newBackupFlagSet(&bo)
	if err := bfs.Parse([]string{"-store", "a.db", "-out", "a.ygo", "-force", "-log", "json"}); err != nil {
		t.Fatalf("backup parse: %v", err)
	}
	if bo.store != "a.db" || bo.out != "a.ygo" || !bo.force || bo.log != "json" {
		t.Fatalf("backup opts = %+v", bo)
	}

	var ro restoreOpts
	rfs := newRestoreFlagSet(&ro)
	if err := rfs.Parse([]string{"-in", "a.ygo", "-store", "a.db"}); err != nil {
		t.Fatalf("restore parse: %v", err)
	}
	if ro.in != "a.ygo" || ro.store != "a.db" || ro.force {
		t.Fatalf("restore opts = %+v", ro)
	}
}

// The package doc must describe both maintenance subcommands.
func TestUnit_PackageDoc_ListsMaintenanceCommands(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	doc := string(src)
	for _, want := range []string{"ygo-server backup", "ygo-server restore", "-force", "SHA-256"} {
		if !strings.Contains(doc, want) {
			t.Fatalf("package doc missing %q", want)
		}
	}
}

// A restored database also works through the same LegacyAdapter the server
// wires (LoadDoc), proving the post-restore server-start path.
func TestRealMain_RestoredDBServesViaAdapter(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data.db")
	seedCLIDB(t, dbPath)
	backupPath := filepath.Join(dir, "data.ygo")
	if code := realMain([]string{"backup", "-store", dbPath, "-out", backupPath}); code != 0 {
		t.Fatalf("backup code = %d", code)
	}
	dst := filepath.Join(dir, "restored.db")
	if code := realMain([]string{"restore", "-in", backupPath, "-store", dst}); code != 0 {
		t.Fatalf("restore code = %d", code)
	}
	s, err := sqlite.Open(dst)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	adapter := persistence.NewLegacyAdapter(s)
	loaded, err := adapter.LoadDoc("doc")
	if err != nil || loaded == nil {
		t.Fatalf("LoadDoc: blob=%v err=%v", loaded, err)
	}
	d := crdt.New()
	if err := crdt.ApplyUpdateV1(d, loaded, nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := d.GetText("t").ToString(); got != "ab" {
		t.Fatalf("adapter loaded text = %q, want ab", got)
	}
}

// Serve-mode behavior is unchanged: the default flags still parse and a server
// with an empty store still starts and shuts down (regression guard around the
// subcommand dispatch).
func TestRealMain_ServeModeUnaffected(t *testing.T) {
	// An unrecognized serve-mode invocation still takes the serve parse path
	// (exit 2 from the serve FlagSet), not a subcommand path.
	if code := realMain([]string{"--definitely-not-a-flag"}); code != 2 {
		t.Fatalf("serve bad flag code = %d, want 2", code)
	}
	if code := realMain([]string{"-h"}); code != 0 {
		t.Fatalf("serve -h code = %d, want 0", code)
	}

	cfg := Config{Addr: "127.0.0.1:0", Store: "", MaxMessageBytes: 1 << 20}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	ready := make(chan struct{})
	go func() { errCh <- run(ctx, cfg, ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("in-memory server did not become ready")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not shut down")
	}
}
