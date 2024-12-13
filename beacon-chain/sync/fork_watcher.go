package sync

import (
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v5/network/forks"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
)

// Is a background routine that observes for new incoming forks. Depending on the epoch
// it will be in charge of subscribing/unsubscribing the relevant topics at the fork boundaries.
func (s *Service) forkWatcher() {
	slotTicker := slots.NewSlotTicker(s.cfg.clock.GenesisTime(), params.BeaconConfig().SecondsPerSlot)
	for {
		select {
		// In the event of a node restart, we will still end up subscribing to the correct
		// topics during/after the fork epoch. This routine is to ensure correct
		// subscriptions for nodes running before a fork epoch.
		case currSlot := <-slotTicker.C():
			currEpoch := slots.ToEpoch(currSlot)
			if err := s.registerForUpcomingFork(currEpoch); err != nil {
				log.WithError(err).Error("Unable to check for fork in the next epoch")
				continue
			}
			if err := s.deregisterFromPastFork(currEpoch); err != nil {
				log.WithError(err).Error("Unable to check for fork in the previous epoch")
				continue
			}
			// Broadcast BLS changes at the Capella fork boundary
			s.broadcastBLSChanges(currSlot)

		case <-s.ctx.Done():
			log.Debug("Context closed, exiting goroutine")
			slotTicker.Done()
			return
		}
	}
}

// Register appropriate gossip and RPC topic if there is a fork in the next epoch.
func (s *Service) registerForUpcomingFork(currentEpoch primitives.Epoch) error {
	// Get the genesis validators root.
	genesisValidatorsRoot := s.cfg.clock.GenesisValidatorsRoot()

	// Check if there is a fork in the next epoch.
	isForkNextEpoch, err := forks.IsForkNextEpoch(s.cfg.clock.GenesisTime(), genesisValidatorsRoot[:])
	if err != nil {
		return errors.Wrap(err, "Could not retrieve next fork epoch")
	}

	// Exit early if there is no fork in the next epoch.
	if !isForkNextEpoch {
		return nil
	}

	// Compute the next epoch.
	nextEpoch := currentEpoch + 1

	// Get the fork digest for the next epoch.
	digest, err := forks.ForkDigestFromEpoch(nextEpoch, genesisValidatorsRoot[:])
	if err != nil {
		return errors.Wrap(err, "could not retrieve fork digest")
	}

	// Exit early if the topics for the next epoch are already registered.
	// It likely to be the case for all slots of the epoch that are not the first one.
	if s.subHandler.digestExists(digest) {
		return nil
	}

	// Register the subscribers (gossipsub) for the next epoch.
	s.registerSubscribers(nextEpoch, digest)

	// Get the handlers for the current and next fork.
	currentHandlerByTopic, err := s.rpcHandlerByTopicFromEpoch(currentEpoch)
	if err != nil {
		return errors.Wrap(err, "RPC handler by topic")
	}

	nextHandlerByTopic, err := s.rpcHandlerByTopicFromEpoch(nextEpoch)
	if err != nil {
		return errors.Wrap(err, "RPC handler by topic")
	}

	// Compute newsly added topics.
	newRPCHandlerByTopic := addedRPCHandlerByTopic(currentHandlerByTopic, nextHandlerByTopic)

	// Register the new RPC handlers.
	for topic, handler := range newRPCHandlerByTopic {
		s.registerRPC(topic, handler)
	}

	return nil
}

// deregisterFromPastFork checks if there was a fork in the previous epoch,
// and if there was then we deregister the gossipsub topics from that particular fork,
// and the RPC handlers that are no longer relevant.
func (s *Service) deregisterFromPastFork(currentEpoch primitives.Epoch) error {
	// Extract the genesis validators root.
	genesisValidatorsRoot := s.cfg.clock.GenesisValidatorsRoot()

	// Get the fork.
	currentFork, err := forks.Fork(currentEpoch)
	if err != nil {
		return errors.Wrap(err, "genesis validators root")
	}

	// If we are still in our genesis fork version then exit early.
	if currentFork.Epoch == params.BeaconConfig().GenesisEpoch {
		return nil
	}

	epochAfterFork := currentFork.Epoch + 1

	// Start de-registring if the current epoch is the first epoch after the fork.
	if epochAfterFork == currentEpoch {
		// Look at the previous fork's digest.
		epochBeforeFork := currentFork.Epoch - 1

		previousDigest, err := forks.ForkDigestFromEpoch(epochBeforeFork, genesisValidatorsRoot[:])
		if err != nil {
			return errors.Wrap(err, "fork digest from epoch")
		}

		// Exit early if there are no topics with that particular digest.
		if !s.subHandler.digestExists(previousDigest) {
			return nil
		}

		// Compute the RPC handlers that are no longer needed.
		currentHandlerByTopic, err := s.rpcHandlerByTopicFromEpoch(currentEpoch)
		if err != nil {
			return errors.Wrap(err, "RPC handler by topic from epoch")
		}

		nextHandlerByTopic, err := s.rpcHandlerByTopicFromEpoch(epochAfterFork)
		if err != nil {
			return errors.Wrap(err, "RPC handler by topic from epoch")
		}

		topicsToRemove := removedRPCTopics(currentHandlerByTopic, nextHandlerByTopic)
		for topic := range topicsToRemove {
			fullTopic := topic + s.cfg.p2p.Encoding().ProtocolSuffix()
			s.cfg.p2p.Host().RemoveStreamHandler(protocol.ID(fullTopic))
		}

		// Run through all our current active topics and see
		// if there are any subscriptions to be removed.
		for _, t := range s.subHandler.allTopics() {
			retDigest, err := p2p.ExtractGossipDigest(t)
			if err != nil {
				log.WithError(err).Error("Could not retrieve digest")
				continue
			}
			if retDigest == previousDigest {
				s.unSubscribeFromTopic(t)
			}
		}
	}

	return nil
}
