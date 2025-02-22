package sync

import (
	"fmt"
	"strings"
	"time"

	"github.com/prysmaticlabs/prysm/v5/async"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/feed"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/feed/state"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/peerdas"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
)

const updateToAdvertiseCustodyGroupCountPeriod = 1 * time.Minute

func (s *Service) maintainValidatorsCustody() {
	async.RunEvery(s.ctx, updateToAdvertiseCustodyGroupCountPeriod, s.updateToAdvertiseCustodyGroupCount)

	stateCh := make(chan *feed.Event, 1)
	stateSub := s.cfg.stateNotifier.StateFeed().Subscribe(stateCh)
	defer stateSub.Unsubscribe()

	latestProcessedEpoch := params.BeaconConfig().FarFutureEpoch

	for {
		select {
		case event := <-stateCh:
			latestProcessedEpoch = s.handleEvent(event, latestProcessedEpoch)
		case err := <-stateSub.Err():
			log.WithError(err).Error("DataColumnSampler1D subscription to state feed failed")
		case <-s.ctx.Done():
			log.Debug("Context canceled, exiting data column sampling loop.")
			return
		}
	}
}

// updateToAdvertiseCustodyGroupCount updates the custody group count to advertise.
func (s *Service) updateToAdvertiseCustodyGroupCount() {
	// Retrieve the registered topics, and store them in a map for quick lookup.
	registeredTopicsSlice := s.subHandler.allTopics()
	registeredTopics := make(map[string]bool, len(registeredTopicsSlice))

	for _, topic := range registeredTopicsSlice {
		topicMessage := extractGossipMessage(topic)
		registeredTopics[topicMessage] = true
	}

	// Get the node ID.
	nodeID := s.cfg.p2p.NodeID()

	peerdas.CustodyGroupCountMut.Lock()
	defer peerdas.CustodyGroupCountMut.Unlock()

	// Get the custody group count.
	targetCustodyGroupCount := peerdas.TargetCustodyGroupCount.Get()

	// Get the peerDAS info.
	info, _, err := peerdas.Info(nodeID, targetCustodyGroupCount)
	if err != nil {
		log.WithError(err).Error("Failed to get peerDAS info")
		return
	}

	for column := range info.CustodyColumns {
		topicMessage := fmt.Sprintf(p2p.GossipDataColumnSidecarMessage+"_%d", column)
		if !registeredTopics[topicMessage] {
			// At least one data column subnet we should be subscribed to is not.
			return
		}
	}

	// All data column subnets we should be subscribed to are.
	peerdas.ToAdvertiseCustodyGroupCount.Set(targetCustodyGroupCount)
}

// handleEvent handles a state feed event.
func (s *Service) handleEvent(event *feed.Event, latestProcessedEpoch primitives.Epoch) primitives.Epoch {
	// Ignore events that are not block processed events.
	if event.Type != state.BlockProcessed {
		return latestProcessedEpoch
	}

	// Ignore events that do not have the correct data type.
	data, ok := event.Data.(*state.BlockProcessedData)
	if !ok {
		log.Error("Event feed data is not of type *statefeed.BlockProcessedData")
		return latestProcessedEpoch
	}

	// Ignore events that are not verified.
	if !data.Verified {
		return latestProcessedEpoch
	}

	// Return early if this epoch has already been processed.
	epoch := slots.ToEpoch(data.Slot)
	if epoch == latestProcessedEpoch {
		return latestProcessedEpoch
	}

	// Get the indices of the tracked validators.
	indices := s.trackedValidatorsCache.Indices()

	// Write lock custody group count.
	peerdas.CustodyGroupCountMut.Lock()
	defer peerdas.CustodyGroupCountMut.Unlock()

	// Set the validators custody requirement if there are no tracked validators.
	if len(indices) == 0 {
		peerdas.TargetCustodyGroupCount.SetValidatorsCustodyRequirement(0)
		return epoch
	}

	// Get the state for the block root.
	state := s.cfg.stateGen.StateByRootIfCachedNoCopy(data.BlockRoot)
	if state == nil {
		return latestProcessedEpoch
	}

	// Get the validators custody requirement.
	validatorsCustodyRequirement, err := peerdas.ValidatorsCustodyRequirement(state, indices)
	if err != nil {
		log.WithError(err).Error("Failed to get validators custody requirement")
		return latestProcessedEpoch
	}

	// Set the validators custody requirement.
	peerdas.TargetCustodyGroupCount.SetValidatorsCustodyRequirement(validatorsCustodyRequirement)

	return epoch
}

// extractGossipMessage extracts the gossip data column sidecar message from a topic.
func extractGossipMessage(s string) string {
	parts := strings.SplitN(s, "/", 5)

	if len(parts) < 4 {
		return ""
	}

	return parts[3]
}
