package light_client

import (
	"sync"

	"github.com/prysmaticlabs/prysm/v5/consensus-types/interfaces"
)

type Store struct {
	mu sync.RWMutex

	LastLCFinalityUpdate   interfaces.LightClientFinalityUpdate
	LastLCOptimisticUpdate interfaces.LightClientOptimisticUpdate
}

func (s *Store) SetLastLCFinalityUpdate(update interfaces.LightClientFinalityUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastLCFinalityUpdate = update
}

func (s *Store) GetLastLCFinalityUpdate() interfaces.LightClientFinalityUpdate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastLCFinalityUpdate
}

func (s *Store) SetLastLCOptimisticUpdate(update interfaces.LightClientOptimisticUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastLCOptimisticUpdate = update
}

func (s *Store) GetLastLCOptimisticUpdate() interfaces.LightClientOptimisticUpdate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastLCOptimisticUpdate
}
