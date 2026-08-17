package downloadercfg

import (
	"testing"

	"github.com/go-quicktest/qt"
)

func TestDefaultTorrentClientConfigDisablesUTP(t *testing.T) {
	cfg := defaultTorrentClientConfig()
	qt.Check(t, qt.IsTrue(cfg.DisableUTP))
}
