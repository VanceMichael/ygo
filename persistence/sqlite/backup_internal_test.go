package sqlite

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// installBackup is the "build a new store" half of Restore. A failure between
// validation and the atomic rename — here a context already canceled when the
// build begins, modeling a cancel/aborted commit or a disk-full transaction
// start — must delete the temp database and must never create the target.
func TestInstallBackup_FailureCleansTempAndPreservesTarget(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "installed.db")

	// Produce a valid artifact straight from the writer.
	src, err := Open(filepath.Join(t.TempDir(), "src.db"))
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	var buf bytes.Buffer
	if _, err := Backup(context.Background(), src, &buf); err != nil {
		t.Fatalf("seed backup: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	hdr, err := validateArtifact(context.Background(), r)
	if err != nil {
		t.Fatalf("validateArtifact: %v", err)
	}

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := installBackup(cctx, r, dst, hdr); !errors.Is(err, context.Canceled) {
		t.Fatalf("installBackup canceled = %v, want context.Canceled", err)
	}

	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("target created despite failed install: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".ygorestore-*.tmp"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temp database left behind after failed install: %v", matches)
	}
}
