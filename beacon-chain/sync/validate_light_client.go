package sync

import (
	"context"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/interfaces"
	"github.com/prysmaticlabs/prysm/v5/monitoring/tracing"
	"github.com/prysmaticlabs/prysm/v5/monitoring/tracing/trace"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
)

func (s *Service) validateLightClientFinalityUpdate(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	// Validation runs on publish (not just subscriptions), so we should approve any message from
	// ourselves.
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationAccept, nil
	}

	// TODO keep?
	// The head state will be too far away to validate any execution change.
	if s.cfg.initialSync.Syncing() {
		return pubsub.ValidationIgnore, nil
	}

	_, span := trace.StartSpan(ctx, "sync.validateLightClientFinalityUpdate")
	defer span.End()

	log.Info("LC: p2p validateLightClientFinalityUpdate invoked")

	m, err := s.decodePubsubMessage(msg)
	if err != nil {
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}

	update, ok := m.(interfaces.LightClientFinalityUpdate)
	if !ok {
		return pubsub.ValidationReject, errWrongMessage
	}

	maxActiveParticipants := update.SyncAggregate().SyncCommitteeBits.Len()
	numActiveParticipants := update.SyncAggregate().SyncCommitteeBits.Count()
	hasSupermajority := numActiveParticipants*3 >= maxActiveParticipants*2

	last := s.lcStore.LastLCFinalityUpdate
	if last != nil {
		// [IGNORE] The finalized_header.beacon.slot is greater than that of all previously forwarded finality_updates,
		// or it matches the highest previously forwarded slot and also has a sync_aggregate indicating supermajority (> 2/3)
		// sync committee participation while the previously forwarded finality_update for that slot did not indicate supermajority
		slot := last.FinalizedHeader().Beacon().Slot
		lastMaxActiveParticipants := last.SyncAggregate().SyncCommitteeBits.Len()
		lastNumActiveParticipants := last.SyncAggregate().SyncCommitteeBits.Count()
		lastHasSupermajority := lastNumActiveParticipants*3 >= lastMaxActiveParticipants*2

		if update.FinalizedHeader().Beacon().Slot < slot {
			return pubsub.ValidationIgnore, nil
		}
		if update.FinalizedHeader().Beacon().Slot == slot && (lastHasSupermajority || !hasSupermajority) {
			return pubsub.ValidationIgnore, nil
		}
	}
	// [IGNORE] The finality_update is received after the block at signature_slot was given enough time
	// to propagate through the network -- i.e. validate that one-third of finality_update.signature_slot
	// has transpired (SECONDS_PER_SLOT / INTERVALS_PER_SLOT seconds after the start of the slot,
	// with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance)
	earliestValidTime := slots.StartTime(uint64(s.cfg.clock.GenesisTime().Unix()), update.FinalizedHeader().Beacon().Slot).
		Add(time.Second * time.Duration(params.BeaconConfig().SecondsPerSlot/params.BeaconConfig().IntervalsPerSlot)).
		Add(-params.BeaconConfig().MaximumGossipClockDisparityDuration())
	if s.cfg.clock.Now().Before(earliestValidTime) {
		return pubsub.ValidationIgnore, nil
	}

	msg.ValidatorData = update.Proto()

	return pubsub.ValidationAccept, nil
}

func (s *Service) validateLightClientOptimisticUpdate(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	// Validation runs on publish (not just subscriptions), so we should approve any message from
	// ourselves.
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationAccept, nil
	}

	// TODO keep?
	// The head state will be too far away to validate any execution change.
	if s.cfg.initialSync.Syncing() {
		return pubsub.ValidationIgnore, nil
	}

	_, span := trace.StartSpan(ctx, "sync.validateLightClientOptimisticUpdate")
	defer span.End()

	log.Info("LC: p2p validateLightClientOptimisticUpdate invoked")

	m, err := s.decodePubsubMessage(msg)
	if err != nil {
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}

	update, ok := m.(interfaces.LightClientOptimisticUpdate)
	if !ok {
		return pubsub.ValidationReject, errWrongMessage
	}

	last := s.lcStore.LastLCOptimisticUpdate
	if last != nil {
		// [IGNORE] The attested_header.beacon.slot is greater than that of all previously forwarded optimistic_updates
		if update.AttestedHeader().Beacon().Slot <= last.AttestedHeader().Beacon().Slot {
			return pubsub.ValidationIgnore, nil
		}
	}
	// [IGNORE] The optimistic_update is received after the block at signature_slot was given enough time
	// to propagate through the network -- i.e. validate that one-third of optimistic_update.signature_slot
	// has transpired (SECONDS_PER_SLOT / INTERVALS_PER_SLOT seconds after the start of the slot,
	// with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance)
	earliestValidTime := slots.StartTime(uint64(s.cfg.clock.GenesisTime().Unix()), update.AttestedHeader().Beacon().Slot).
		Add(time.Second * time.Duration(params.BeaconConfig().SecondsPerSlot/params.BeaconConfig().IntervalsPerSlot)).
		Add(-params.BeaconConfig().MaximumGossipClockDisparityDuration())
	if s.cfg.clock.Now().Before(earliestValidTime) {
		return pubsub.ValidationIgnore, nil
	}

	msg.ValidatorData = update.Proto()

	return pubsub.ValidationAccept, nil
}
