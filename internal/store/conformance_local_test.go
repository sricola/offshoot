package store_test

import (
	"testing"

	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/store/storetest"
)

func TestLocalConformance(t *testing.T) {
	storetest.RunConformance(t, "", func(t *testing.T) store.Backend {
		b, err := store.NewLocal(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}
