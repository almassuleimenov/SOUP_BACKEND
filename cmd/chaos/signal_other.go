//go:build !unix

package main

import (
	"errors"
	"os"
)

// Windows has no SIGSTOP; zombies are produced by the in-worker freeze hook
// (CHAOS_FREEZE_PROB) instead.
const canPause = false

func pauseProcess(*os.Process) error  { return errors.New("pause not supported on this OS") }
func resumeProcess(*os.Process) error { return nil }
