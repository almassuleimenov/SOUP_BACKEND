//go:build unix

package main

import (
	"os"
	"syscall"
)

const canPause = true

func pauseProcess(p *os.Process) error  { return p.Signal(syscall.SIGSTOP) }
func resumeProcess(p *os.Process) error { return p.Signal(syscall.SIGCONT) }
