//go:build !chaos

package main

import (
	"fmt"
	"os"

	"jobqueue/internal/store"
	"jobqueue/internal/worker"
)

// chaosConfig in the production build: fault-injection switches are not
// compiled in. If any of them is set, refuse to start rather than silently
// ignoring it (someone expected it to do something).
func chaosConfig(*worker.Config, *store.Options) error {
	for _, k := range []string{"UNSAFE_MODE", "CHAOS_FREEZE_PROB", "CHAOS_FREEZE_FOR", "EFFECT_MODE"} {
		if os.Getenv(k) != "" {
			return fmt.Errorf("%s is set, but this worker was built without -tags chaos; fault injection is not available in production builds", k)
		}
	}
	return nil
}
