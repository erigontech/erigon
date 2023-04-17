package jsonrpc

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/zkevm/hex"
)

// ErrNotFound represent a not found error.
var ErrNotFound = errors.New("object not found")

// ErrFilterInvalidPayload indicates there is an invalid payload when creating a filter
var ErrFilterInvalidPayload = errors.New("invalid argument 0: cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other")

// Storage uses memory to store the data
// related to the json rpc server
type Storage struct {
	filters sync.Map
}

// NewStorage creates and initializes an instance of Storage
func NewStorage() *Storage {
	return &Storage{
		filters: sync.Map{},
	}
}

// NewLogFilter persists a new log filter
func (s *Storage) NewLogFilter(wsConn *websocket.Conn, filter LogFilter) (string, error) {
	if filter.BlockHash != nil && (filter.FromBlock != nil || filter.ToBlock != nil) {
		return "", ErrFilterInvalidPayload
	}

	return s.createFilter(FilterTypeLog, filter, wsConn)
}

// NewBlockFilter persists a new block log filter
func (s *Storage) NewBlockFilter(wsConn *websocket.Conn) (string, error) {
	return s.createFilter(FilterTypeBlock, nil, wsConn)
}

// NewPendingTransactionFilter persists a new pending transaction filter
func (s *Storage) NewPendingTransactionFilter(wsConn *websocket.Conn) (string, error) {
	return s.createFilter(FilterTypePendingTx, nil, wsConn)
}

// create persists the filter to the memory and provides the filter id
func (s *Storage) createFilter(t FilterType, parameters interface{}, wsConn *websocket.Conn) (string, error) {
	lastPoll := time.Now().UTC()
	id, err := s.generateFilterID()
	if err != nil {
		return "", fmt.Errorf("failed to generate filter ID: %w", err)
	}
	s.filters.Store(id, &Filter{
		ID:         id,
		Type:       t,
		Parameters: parameters,
		LastPoll:   lastPoll,
		WsConn:     wsConn,
	})

	return id, nil
}

func (s *Storage) generateFilterID() (string, error) {
	r, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	b, err := r.MarshalBinary()
	if err != nil {
		return "", err
	}

	id := hex.EncodeToHex(b)
	return id, nil
}

// GetAllBlockFiltersWithWSConn returns an array with all filter that have
// a web socket connection and are filtering by new blocks
func (s *Storage) GetAllBlockFiltersWithWSConn() ([]*Filter, error) {
	filtersWithWSConn := []*Filter{}
	s.filters.Range(func(key, value any) bool {
		filter := value.(*Filter)
		if filter.WsConn == nil || filter.Type != FilterTypeBlock {
			return true
		}

		f := filter
		filtersWithWSConn = append(filtersWithWSConn, f)
		return true
	})

	return filtersWithWSConn, nil
}

// GetAllLogFiltersWithWSConn returns an array with all filter that have
// a web socket connection and are filtering by new logs
func (s *Storage) GetAllLogFiltersWithWSConn() ([]*Filter, error) {
	filtersWithWSConn := []*Filter{}
	s.filters.Range(func(key, value any) bool {
		filter := value.(*Filter)
		if filter.WsConn == nil || filter.Type != FilterTypeLog {
			return true
		}

		f := filter
		filtersWithWSConn = append(filtersWithWSConn, f)
		return true
	})

	return filtersWithWSConn, nil
}

// GetFilter gets a filter by its id
func (s *Storage) GetFilter(filterID string) (*Filter, error) {
	filter, found := s.filters.Load(filterID)
	if !found {
		return nil, ErrNotFound
	}

	return filter.(*Filter), nil
}

// UpdateFilterLastPoll updates the last poll to now
func (s *Storage) UpdateFilterLastPoll(filterID string) error {
	filterValue, found := s.filters.Load(filterID)
	if !found {
		return ErrNotFound
	}
	filter := filterValue.(*Filter)
	filter.LastPoll = time.Now().UTC()
	s.filters.Store(filterID, filter)
	return nil
}

// UninstallFilter deletes a filter by its id
func (s *Storage) UninstallFilter(filterID string) error {
	_, found := s.filters.Load(filterID)
	if !found {
		return ErrNotFound
	}
	s.filters.Delete(filterID)
	return nil
}

// UninstallFilterByWSConn deletes all filters connected to the provided web socket connection
func (s *Storage) UninstallFilterByWSConn(wsConn *websocket.Conn) error {
	filterIDsToDelete := []string{}
	s.filters.Range(func(key, value any) bool {
		id := key.(string)
		filter := value.(*Filter)
		if filter.WsConn == wsConn {
			filterIDsToDelete = append(filterIDsToDelete, id)
		}
		return true
	})

	for _, filterID := range filterIDsToDelete {
		s.filters.Delete(filterID)
	}

	return nil
}
