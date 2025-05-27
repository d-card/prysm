package sync

import (
	"bytes"
	"context"
	"encoding/hex"
	"sync"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/core/helpers"
	fieldparams "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"

	// ethpb is not directly used by name, but types from it might be used by state.Validators()
	"github.com/OffchainLabs/prysm/v6/beacon-chain/state"
	_ "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/sirupsen/logrus"
)

const trackedValidatorPublicKeyStr = "0x99743c58a2de9946397bc92ddc12f108a71ddb82b61dde2c337d2489cd5d7901b2d045f7a1685b9a08bbfd07a0d12909"
var trackedValidatorPublicKey [fieldparams.BLSPubkeyLength]byte
var trackedValidatorFeatureEnabled bool

func init() {
	pubKeyBytes, err := hex.DecodeString(trackedValidatorPublicKeyStr[2:])
	if err != nil {
		logrus.WithError(err).Error("Failed to decode tracked validator public key, disabling feature.")
		trackedValidatorFeatureEnabled = false
		return
	}
	if len(pubKeyBytes) != fieldparams.BLSPubkeyLength {
		logrus.Errorf("Decoded public key length %d does not match expected length %d, disabling feature.", len(pubKeyBytes), fieldparams.BLSPubkeyLength)
		trackedValidatorFeatureEnabled = false
		return
	}
	copy(trackedValidatorPublicKey[:], pubKeyBytes)
	trackedValidatorFeatureEnabled = true
	logrus.Info("Tracked validator feature enabled for public key: ", trackedValidatorPublicKeyStr)
}

// Returns a slice of subnet IDs that the tracked validator is scheduled for.
func getTrackedValidatorAttestationSubnet(
	s *Service,
	slotForEpochDetermination primitives.Slot, // The slot used to determine current/next epochs for duty scanning.
	rootForCurrentEpochStateQuery [fieldparams.RootLength]byte, // The block root of the state to be used for the current epoch's duty scan.
) []uint64 {
	if !trackedValidatorFeatureEnabled {
		return []uint64{}
	}

	var subnetMutex sync.Mutex
	trackedSubnetsSet := make(map[uint64]struct{})

	ctx := context.Background()
	currentEpochForDutyScan := slots.ToEpoch(slotForEpochDetermination)
	nextEpochForDutyScan := currentEpochForDutyScan + 1
	epochsToScan := []primitives.Epoch{currentEpochForDutyScan, nextEpochForDutyScan}

	for _, epochToQuery := range epochsToScan {
		epochStartSlot, err := slots.EpochStart(epochToQuery)
		if err != nil {
			logrus.WithError(err).WithField("epoch", epochToQuery).Error("Could not get epoch start slot for tracked validator duty check")
			continue
		}

		if epochToQuery == currentEpochForDutyScan && epochStartSlot > slotForEpochDetermination {
			logrus.WithFields(logrus.Fields{
				"slotForEpochDetermination": slotForEpochDetermination,
				"epochToQuery":          epochToQuery,
				"epochStartSlot": epochStartSlot,
			}).Debug("Skipping epoch for tracked validator: slotForEpochDetermination is before this epoch's calculated start slot.")
			continue
		}
		
		var stateForQuery state.BeaconState
		var stateRootUsedStr string // For logging

		if epochToQuery == currentEpochForDutyScan {
			var zeroRootTest [fieldparams.RootLength]byte
			if bytes.Equal(rootForCurrentEpochStateQuery[:], zeroRootTest[:]) {
				logrus.WithFields(logrus.Fields{
					"epochToQuery": epochToQuery,
					"slotForEpochDetermination": slotForEpochDetermination,
				}).Error("Provided rootForCurrentEpochStateQuery is zero; skipping epoch for tracked validator.")
				continue
			}
			beaconStateTmp, errState := s.cfg.stateGen.StateByRoot(ctx, rootForCurrentEpochStateQuery)
			if errState != nil {
				logrus.WithError(errState).WithFields(logrus.Fields{
					"epochToQuery": epochToQuery,
					"rootUsed":  hex.EncodeToString(rootForCurrentEpochStateQuery[:]),
				}).Error("Could not get beacon state for tracked validator (current epoch scan)")
				continue
			}
			if beaconStateTmp == nil || beaconStateTmp.IsNil() {
				logrus.WithFields(logrus.Fields{
					"epochToQuery": epochToQuery,
					"rootUsed":  hex.EncodeToString(rootForCurrentEpochStateQuery[:]),
				}).Error("Nil or empty beacon state for tracked validator (current epoch scan)")
				continue
			}
			stateForQuery = beaconStateTmp
			stateRootUsedStr = hex.EncodeToString(rootForCurrentEpochStateQuery[:])
		} else { // nextEpochForDutyScan
			// Get head root to use as basis for Ancestor lookup for the next epoch's start slot state.
			headRootBytes, errHead := s.cfg.chain.HeadRoot(ctx)
			if errHead != nil {
				logrus.WithError(errHead).WithField("epochToQuery", epochToQuery).Error("Could not get head root for Ancestor lookup (next epoch scan)")
				continue
			}
			if len(headRootBytes) != 32 {
				logrus.WithFields(logrus.Fields{"epochToQuery": epochToQuery, "len": len(headRootBytes)}).Error("Invalid head root length (next epoch scan)")
				continue
			}
			var headRoot [32]byte
			copy(headRoot[:], headRootBytes)

			var zeroHeadRoot [32]byte
			if bytes.Equal(headRoot[:], zeroHeadRoot[:]) {
				logrus.WithField("epochToQuery", epochToQuery).Error("Zero head root for Ancestor lookup (next epoch scan)")
				continue
			}

			ancestorRootBytes, errAncestor := s.cfg.chain.Ancestor(ctx, headRoot[:], epochStartSlot)
			if errAncestor != nil {
				logrus.WithError(errAncestor).WithFields(logrus.Fields{
					"targetSlotForState": epochStartSlot,
					"epochToQuery":    epochToQuery,
					"headRootUsed": hex.EncodeToString(headRoot[:]),
				}).Error("Could not get ancestor block root for state (next epoch scan)")
				continue
			}
			if len(ancestorRootBytes) != 32 {
				logrus.WithFields(logrus.Fields{
					"targetSlotForState": epochStartSlot, 
					"epochToQuery": epochToQuery, 
					"len": len(ancestorRootBytes),
				}).Error("Ancestor block root has invalid length (next epoch scan)")
				continue
			}
			var ancestorRoot [32]byte
			copy(ancestorRoot[:], ancestorRootBytes)

			var zeroAncestorRoot [32]byte
			if bytes.Equal(ancestorRoot[:], zeroAncestorRoot[:]) {
				logrus.WithFields(logrus.Fields{
					"targetSlotForState": epochStartSlot,
					"epochToQuery":    epochToQuery,
				}).Error("Zero ancestor block root for state (next epoch scan)")
				continue
			}

			beaconStateTmp, errState := s.cfg.stateGen.StateByRoot(ctx, ancestorRoot)
			if errState != nil {
				logrus.WithError(errState).WithFields(logrus.Fields{
					"epochToQuery": epochToQuery,
					"rootUsed":  hex.EncodeToString(ancestorRoot[:]),
				}).Error("Could not get beacon state for tracked validator (next epoch scan)")
				continue
			}
			if beaconStateTmp == nil || beaconStateTmp.IsNil() {
				logrus.WithFields(logrus.Fields{
					"epochToQuery": epochToQuery,
					"rootUsed":  hex.EncodeToString(ancestorRoot[:]),
				}).Error("Nil or empty beacon state for tracked validator (next epoch scan)")
				continue
			}
			stateForQuery = beaconStateTmp
			stateRootUsedStr = hex.EncodeToString(ancestorRoot[:])
		}
		
		valIdx, found := getValidatorIndexFromState(stateForQuery, trackedValidatorPublicKey) // Removed ctx
		if !found {
			logrus.WithFields(logrus.Fields{
				"epochToQuery":    epochToQuery,
				"stateSlotUsed":     stateForQuery.Slot(),
				"stateRootUsed": stateRootUsedStr,
				"pubkey":   trackedValidatorPublicKeyStr,
			}).Debug("Tracked validator not found in beacon state for duty check")
			continue
		}
		
		assignments, err := helpers.CommitteeAssignments(ctx, stateForQuery, epochToQuery, []primitives.ValidatorIndex{valIdx})
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"epochToQuery":          epochToQuery,
				"validatorIndex": valIdx,
				"stateRootUsed":      stateRootUsedStr,
				"stateSlotUsed":      stateForQuery.Slot(),
			}).Error("Could not get committee assignments for tracked validator")
			continue
		}

		assignment, ok := assignments[valIdx]
		if !ok || assignment == nil {
			logrus.WithFields(logrus.Fields{
				"epochToQuery":          epochToQuery,
				"validatorIndex": valIdx,
				"pubkey":         trackedValidatorPublicKeyStr,
				"stateRootUsed":      stateRootUsedStr,
				"stateSlotUsed":      stateForQuery.Slot(),
			}).Debug("Tracked validator has no attestation assignment in this epoch's state")
			continue
		}

		assignmentEpochStartSlot, _ := slots.EpochStart(epochToQuery)
		assignmentEpochEndSlot := assignmentEpochStartSlot + params.BeaconConfig().SlotsPerEpoch - 1

		if assignment.AttesterSlot < assignmentEpochStartSlot || assignment.AttesterSlot > assignmentEpochEndSlot {
			logrus.WithFields(logrus.Fields{
				"epochToQuery":            epochToQuery,
				"validatorIndex":   valIdx,
				"assignmentSlot":   assignment.AttesterSlot,
				"stateSlotUsed":   stateForQuery.Slot(),
				"epochStartSlotForDuty":   assignmentEpochStartSlot,
				"epochEndSlotForDuty":     assignmentEpochEndSlot,
				"trackedValidator": trackedValidatorPublicKeyStr,
			}).Warn("Tracked validator's attestation assignment slot is outside the queried epoch. Skipping this assignment.")
			continue
		}
		
		committeeIndex := assignment.CommitteeIndex
		calculatedSubnetID := committeeIndex % primitives.CommitteeIndex(params.BeaconConfig().AttestationSubnetCount)

		logrus.WithFields(logrus.Fields{
			"validatorIndex": valIdx,
			"pubkey":         trackedValidatorPublicKeyStr,
			"dutyEpoch":      epochToQuery,
			"dutySlot":       assignment.AttesterSlot,
			"committeeIndex": committeeIndex,
			"subnetID":       calculatedSubnetID,
			"stateSlotUsed": stateForQuery.Slot(),
		}).Infof("Found attestation duty for tracked validator, adding subnet %d", calculatedSubnetID)

		subnetMutex.Lock()
		trackedSubnetsSet[uint64(calculatedSubnetID)] = struct{}{}
		subnetMutex.Unlock()
	} // End of epoch iteration

	// Convert set to slice before returning
	finalTrackedSubnets := make([]uint64, 0, len(trackedSubnetsSet))
	for subnetID := range trackedSubnetsSet {
		finalTrackedSubnets = append(finalTrackedSubnets, subnetID)
	}
	return finalTrackedSubnets
}

func getValidatorIndexFromState(st state.BeaconState, pubkey [fieldparams.BLSPubkeyLength]byte) (primitives.ValidatorIndex, bool) {
	validators := st.Validators()
	for i, v := range validators {
		if v == nil {
			continue
		}
		if bytes.Equal(v.PublicKey, pubkey[:]) { 
			return primitives.ValidatorIndex(i), true
		}
	}
	return 0, false
}

func newPersistentAndTrackedValidatorSubnetIndices(
	s *Service,
	currentSlot primitives.Slot,
	blockRoot [fieldparams.RootLength]byte, // blockRoot for the state at currentSlot
) []uint64 {
	// Get the base persistent and aggregator subnets
	baseSubnetsSlice := s.persistentAndAggregatorSubnetIndices(currentSlot) // This is []uint64

	// Use a map to merge and ensure uniqueness
	combinedSubnetsSet := make(map[uint64]struct{})
	for _, subnetID := range baseSubnetsSlice {
		combinedSubnetsSet[subnetID] = struct{}{}
	}

	if !trackedValidatorFeatureEnabled {
		// Convert map back to slice for return
		finalSubnetsSlice := make([]uint64, 0, len(combinedSubnetsSet))
		for subnetID := range combinedSubnetsSet {
			finalSubnetsSlice = append(finalSubnetsSlice, subnetID)
		}
		return finalSubnetsSlice
	}

	// Get subnets for the tracked validator
	validatorSpecificSubnetsSlice := getTrackedValidatorAttestationSubnet(s, currentSlot, blockRoot) // This now returns []uint64

	// Merge the validator-specific subnets into the set
	for _, subnetID := range validatorSpecificSubnetsSlice {
		if _, exists := combinedSubnetsSet[subnetID]; !exists {
			combinedSubnetsSet[subnetID] = struct{}{}
			logrus.WithFields(logrus.Fields{
				"subnetID":    subnetID,
				"currentSlot": currentSlot,
				"source":      "trackedValidator",
			}).Info("Adding subnet for tracked validator to combined list")
		} else {
			logrus.WithFields(logrus.Fields{
				"subnetID":    subnetID,
				"currentSlot": currentSlot,
				"source":      "trackedValidator",
			}).Debug("Tracked validator subnet already present in persistent/aggregator list")
		}
	}

	// Convert the final combined set back to a slice
	finalCombinedSlice := make([]uint64, 0, len(combinedSubnetsSet))
	for subnetID := range combinedSubnetsSet {
		finalCombinedSlice = append(finalCombinedSlice, subnetID)
	}
	return finalCombinedSlice
} 