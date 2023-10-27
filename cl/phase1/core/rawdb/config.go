package rawdb

import (
	"encoding/json"
	"math"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type BeaconDataConfig struct {
	BackFillingAmount   uint64 `json:"backFillingAmount"` // it is string to handle all/minimal.
	SlotPerRestorePoint uint64 `json:"sprp"`              // TODO
}

var beaconDataKey = []byte("beaconData")

// Configurations for beacon database config
var BeaconDataConfigurations map[string]*BeaconDataConfig = map[string]*BeaconDataConfig{
	"full": {
		BackFillingAmount:   math.MaxUint64,
		SlotPerRestorePoint: 0,
	},
	"minimal": {
		BackFillingAmount:   500_000,
		SlotPerRestorePoint: 0,
	},
	"light": {
		BackFillingAmount:   0,
		SlotPerRestorePoint: 0,
	},
}

func WriteBeaconDataConfig(tx kv.Putter, cfg *BeaconDataConfig) error {
	var (
		data []byte
		err  error
	)
	if data, err = json.Marshal(cfg); err != nil {
		return err
	}
	return tx.Put(kv.DatabaseInfo, beaconDataKey, data)
}

func ReadBeaconDataConfig(tx kv.Getter) (*BeaconDataConfig, error) {
	var (
		data []byte
		err  error
		cfg  = &BeaconDataConfig{}
	)
	if data, err = tx.GetOne(kv.DatabaseInfo, beaconDataKey); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	if err = json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
