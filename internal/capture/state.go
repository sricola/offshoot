package capture

import (
	"encoding/json"
	"os"
)

type State struct {
	Off   int64  `json:"off"`
	Salt1 uint32 `json:"salt1"`
	Salt2 uint32 `json:"salt2"`
}

func LoadState(path string) (State, bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, false, nil // corrupt state ⇒ treat as absent ⇒ rebase
	}
	return s, true, nil
}

func SaveState(path string, s State) error {
	b, _ := json.Marshal(s)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
