package decodedstate

import (
	"errors"

	"github.com/erigontech/erigon/common"
)

var ErrDisabled = errors.New("decoded state is disabled")

// rpcHandler implements RPCHandler by delegating to a Store.
type rpcHandler struct {
	store Store
	cfg   Config
}

// NewRPCHandler creates an RPCHandler wrapping the given Store and Config.
func NewRPCHandler(store Store, cfg Config) RPCHandler {
	return &rpcHandler{store: store, cfg: cfg}
}

func (h *rpcHandler) EnumerateMappingKeys(contract common.Address, mappingSlot common.Hash, blockNum *uint64) ([]common.Hash, error) {
	if !h.cfg.Enabled {
		return nil, ErrDisabled
	}
	if blockNum == nil {
		return h.store.EnumerateKeys(contract, mappingSlot)
	}
	return h.store.EnumerateKeysAsOf(contract, mappingSlot, *blockNum)
}

func (h *rpcHandler) GetDecodedStorage(contract common.Address, blockNum *uint64) (map[common.Hash][]DecodedEntry, error) {
	if !h.cfg.Enabled {
		return nil, ErrDisabled
	}
	if blockNum == nil {
		return h.store.GetDecodedStorage(contract)
	}
	return h.store.GetDecodedStorageAsOf(contract, *blockNum)
}

func (h *rpcHandler) GetMappingValue(contract common.Address, mappingSlot, key common.Hash, blockNum *uint64) (common.Hash, error) {
	if !h.cfg.Enabled {
		return common.Hash{}, ErrDisabled
	}
	if blockNum == nil {
		val, found, err := h.store.GetLatest(contract, mappingSlot, key)
		if err != nil {
			return common.Hash{}, err
		}
		if !found {
			return common.Hash{}, nil
		}
		return val, nil
	}
	val, found, err := h.store.GetAsOf(contract, mappingSlot, key, *blockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if !found {
		return common.Hash{}, nil
	}
	return val, nil
}

func (h *rpcHandler) EnumerateMappingKeysAtTx(contract common.Address, mappingSlot common.Hash, txNumber uint64) ([]common.Hash, error) {
	if !h.cfg.Enabled {
		return nil, ErrDisabled
	}
	return h.store.EnumerateKeysAsOf(contract, mappingSlot, txNumber)
}

func (h *rpcHandler) GetDecodedStorageAtTx(contract common.Address, txNumber uint64) (map[common.Hash][]DecodedEntry, error) {
	if !h.cfg.Enabled {
		return nil, ErrDisabled
	}
	return h.store.GetDecodedStorageAsOf(contract, txNumber)
}

func (h *rpcHandler) GetMappingValueAtTx(contract common.Address, mappingSlot, key common.Hash, txNumber uint64) (common.Hash, error) {
	if !h.cfg.Enabled {
		return common.Hash{}, ErrDisabled
	}
	val, found, err := h.store.GetAsOf(contract, mappingSlot, key, txNumber)
	if err != nil {
		return common.Hash{}, err
	}
	if !found {
		return common.Hash{}, nil
	}
	return val, nil
}
