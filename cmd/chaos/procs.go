package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// proc is a child process we can kill.
type proc struct {
	id   string
	cmd  *exec.Cmd
	done chan struct{}
}

// fleet keeps exactly n worker processes alive and lets the chaos loop kill
// or pause random members.
type fleet struct {
	mu      sync.Mutex
	bin     string
	env     []string
	logFile *os.File
	n       int
	gen     int
	procs   map[string]*proc
	stopped bool

	kills, stops, respawns, crashes atomic.Int64
}

func newFleet(bin string, env []string, logFile *os.File, n int) *fleet {
	return &fleet{bin: bin, env: env, logFile: logFile, n: n, procs: map[string]*proc{}}
}

func startProc(bin, id string, env []string, logFile *os.File) (*proc, error) {
	cmd := exec.Command(bin)
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", id, err)
	}
	p := &proc{id: id, cmd: cmd, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait() // exit status is irrelevant: we kill them on purpose
		close(p.done)
	}()
	return p, nil
}

// fill starts workers until n are alive.
func (f *fleet) fill() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped {
		return nil
	}
	for len(f.procs) < f.n {
		f.gen++
		id := fmt.Sprintf("w%03d", f.gen)
		env := append(append([]string{}, f.env...), "WORKER_ID="+id)
		p, err := startProc(f.bin, id, env, f.logFile)
		if err != nil {
			return err
		}
		f.procs[id] = p
		if f.gen > f.n {
			f.respawns.Add(1)
		}
		go f.watch(p)
	}
	return nil
}

// watch removes a worker that exited on its own (a crash we did not cause
// is itself a finding, so it is counted separately) and replaces it.
func (f *fleet) watch(p *proc) {
	<-p.done
	f.mu.Lock()
	_, stillListed := f.procs[p.id]
	delete(f.procs, p.id)
	stopped := f.stopped
	f.mu.Unlock()
	if stillListed && !stopped {
		f.crashes.Add(1)
		logf("UNEXPECTED: worker %s exited on its own", p.id)
		_ = f.fill()
	}
}

func (f *fleet) random() *proc {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.procs) == 0 {
		return nil
	}
	i := rand.N(len(f.procs))
	for _, p := range f.procs {
		if i == 0 {
			return p
		}
		i--
	}
	return nil
}

// killRandom does kill -9 on a random worker and immediately starts a
// replacement, so the fleet size stays constant.
func (f *fleet) killRandom() string {
	p := f.random()
	if p == nil {
		return ""
	}
	f.mu.Lock()
	delete(f.procs, p.id)
	f.mu.Unlock()
	_ = p.cmd.Process.Kill() // SIGKILL on Unix, TerminateProcess on Windows
	<-p.done
	f.kills.Add(1)
	if err := f.fill(); err != nil {
		logf("respawn failed: %v", err)
	}
	return p.id
}

// pauseRandom freezes a random worker for d (SIGSTOP/SIGCONT) without telling
// it: the real "zombie that comes back after its lease expired".
func (f *fleet) pauseRandom(d time.Duration) string {
	p := f.random()
	if p == nil {
		return ""
	}
	if err := pauseProcess(p.cmd.Process); err != nil {
		return ""
	}
	f.stops.Add(1)
	go func() {
		time.Sleep(d)
		_ = resumeProcess(p.cmd.Process)
	}()
	return p.id
}

func (f *fleet) stopAll() {
	f.mu.Lock()
	f.stopped = true
	ps := make([]*proc, 0, len(f.procs))
	for _, p := range f.procs {
		ps = append(ps, p)
	}
	f.procs = map[string]*proc{}
	f.mu.Unlock()
	for _, p := range ps {
		_ = resumeProcess(p.cmd.Process)
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}
