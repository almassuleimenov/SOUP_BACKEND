package api

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// payloadValidators maps each job type the workers can execute to a check of
// its payload. A job of an unknown type, or with a payload the worker cannot
// parse, is rejected with 400 at the API instead of being accepted (201) and
// then failing every attempt in a worker.
var payloadValidators = map[string]func(json.RawMessage) error{
	"train": validateTrain,
}

// validateTrain checks the fields the emulated training worker interprets.
// Other fields are allowed and passed through untouched (real training
// parameters live there).
func validateTrain(raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("payload must be a JSON object")
	}
	intField := func(name string, maxV int64) error {
		v, ok := fields[name]
		if !ok {
			return nil
		}
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.UseNumber()
		var n json.Number
		if err := dec.Decode(&n); err != nil {
			return fmt.Errorf("%s must be an integer", name)
		}
		i, err := n.Int64()
		if err != nil {
			return fmt.Errorf("%s must be an integer", name)
		}
		if i < 0 || i > maxV {
			return fmt.Errorf("%s must be between 0 and %d", name, maxV)
		}
		return nil
	}
	if err := intField("duration_ms", 24*60*60*1000); err != nil {
		return err
	}
	return intField("fail_attempts", maxMaxAttempts)
}
