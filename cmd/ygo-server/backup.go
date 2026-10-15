package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/reearth/ygo/persistence/sqlite"
)

// Maintenance subcommands. Unlike the default (serve) mode these perform ONE
// storage operation and exit: they never open a listener, construct the
// websocket server, or attach a Relay. They exist so an operator migrating a
// collaboration service, or retaining data before maintenance, can produce a
// verifiable, restorable backup from ygo-server itself instead of copying a
// SQLite file while its WAL is live.

// backupOpts is the parsed "ygo-server backup" command line.
type backupOpts struct {
	store string
	out   string
	force bool
	log   string
}

// restoreOpts is the parsed "ygo-server restore" command line.
type restoreOpts struct {
	in    string
	store string
	force bool
	log   string
}

// newBackupFlagSet declares the backup subcommand flags against o.
func newBackupFlagSet(o *backupOpts) *flag.FlagSet {
	fs := flag.NewFlagSet("ygo-server backup", flag.ContinueOnError)
	fs.StringVar(&o.store, "store", "", "SQLite database file to back up (required; in-memory stores cannot be backed up)")
	fs.StringVar(&o.out, "out", "", "path of the portable backup artifact to write (required)")
	fs.BoolVar(&o.force, "force", false, "replace -out if a backup file already exists (the existing file is only replaced after a successful capture)")
	fs.StringVar(&o.log, "log", "text", `log format: "text" or "json"`)
	return fs
}

// newRestoreFlagSet declares the restore subcommand flags against o.
func newRestoreFlagSet(o *restoreOpts) *flag.FlagSet {
	fs := flag.NewFlagSet("ygo-server restore", flag.ContinueOnError)
	fs.StringVar(&o.in, "in", "", "backup artifact to restore (required)")
	fs.StringVar(&o.store, "store", "", "SQLite database file to create (required)")
	fs.BoolVar(&o.force, "force", false, "replace -store if it (or its -wal/-shm sidecars) already exists")
	fs.StringVar(&o.log, "log", "text", `log format: "text" or "json"`)
	return fs
}

// maintenanceContext is canceled by SIGINT/SIGTERM, mirroring serve mode so a
// large capture/restore aborts cleanly and leaves both databases intact.
func maintenanceContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// runBackupCmd parses and executes "ygo-server backup". Exit codes match
// realMain: 0 success, 1 runtime error, 2 bad usage.
func runBackupCmd(args []string) int {
	var o backupOpts
	fs := newBackupFlagSet(&o)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(fs.Output(), "unexpected arguments: %v\n", fs.Args())
		fs.Usage()
		return 2
	}
	if o.store == "" || o.out == "" {
		fmt.Fprintln(fs.Output(), "backup requires -store and -out")
		fs.Usage()
		return 2
	}
	log := newLogger(o.log)

	ctx, stop := maintenanceContext()
	defer stop()
	info, err := sqlite.BackupFile(ctx, o.store, o.out, o.force)
	if err != nil {
		log.Error("backup failed", "err", err, "store", o.store, "out", o.out)
		return 1
	}
	log.Info("backup complete", "store", o.store, "out", o.out,
		"rooms", info.Rooms, "format", fmt.Sprintf("v%d", info.FormatVersion),
		"captured_at", info.CreatedAt.Format("2006-01-02T15:04:05Z07:00"))
	return 0
}

// runRestoreCmd parses and executes "ygo-server restore". It validates the
// whole artifact before touching -store; with a non-empty target it refuses
// unless -force is set.
func runRestoreCmd(args []string) int {
	var o restoreOpts
	fs := newRestoreFlagSet(&o)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(fs.Output(), "unexpected arguments: %v\n", fs.Args())
		fs.Usage()
		return 2
	}
	if o.store == "" || o.in == "" {
		fmt.Fprintln(fs.Output(), "restore requires -in and -store")
		fs.Usage()
		return 2
	}
	log := newLogger(o.log)

	ctx, stop := maintenanceContext()
	defer stop()
	info, err := sqlite.RestoreFile(ctx, o.in, o.store, o.force)
	if err != nil {
		log.Error("restore failed", "err", err, "in", o.in, "store", o.store)
		return 1
	}
	log.Info("restore complete", "in", o.in, "store", o.store,
		"rooms", info.Rooms, "format", fmt.Sprintf("v%d", info.FormatVersion),
		"captured_at", info.CreatedAt.Format("2006-01-02T15:04:05Z07:00"))
	return 0
}
