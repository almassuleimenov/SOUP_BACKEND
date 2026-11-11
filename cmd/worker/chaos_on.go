//go:build chaos

package main

import (
	"fmt"
	"os"

	"jobqueue/internal/config"
	"jobqueue/internal/store"
	"jobqueue/internal/worker"
)

// chaosConfig enables fault injection and negative controls. Compiled only
// with `-tags chaos` (the worker-chaos binary the harness spawns); the
// production worker cannot be switched into these modes by an env var.
func chaosConfig(cfg *worker.Config, opts *store.Options) error {
	freeze, err := config.Float("CHAOS_FREEZE_PROB", 0)
	if err != nil {
		return err
	}
	freezeFor, err := config.Duration("CHAOS_FREEZE_FOR", 2*cfg.Lease)
	if err != nil {
		return err
	}
	cfg.FreezeProb, cfg.FreezeFor = freeze, freezeFor

	cfg.EffectMode = worker.EffectMode(config.String("EFFECT_MODE", string(worker.EffectInTx)))
	if cfg.EffectMode != worker.EffectInTx && cfg.EffectMode != worker.EffectExternal {
		return fmt.Errorf("EFFECT_MODE must be tx or external")
	}
	switch m := os.Getenv("UNSAFE_MODE"); m {
	case "":
	case "no-fence":
		opts.UnsafeNoFence = true
	case "racy-claim":
		opts.UnsafeRacyClaim = true
	default:
		return fmt.Errorf("unknown UNSAFE_MODE %q", m)
	}
	return nil
}
