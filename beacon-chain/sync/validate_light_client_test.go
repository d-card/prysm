package sync

import (
	"bytes"
	"context"
	"testing"
	"time"

	mock "github.com/OffchainLabs/prysm/v6/beacon-chain/blockchain/testing"
	lightClient "github.com/OffchainLabs/prysm/v6/beacon-chain/core/light-client"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	p2ptest "github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/startup"
	mockSync "github.com/OffchainLabs/prysm/v6/beacon-chain/sync/initial-sync/testing"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v6/runtime/version"
	"github.com/OffchainLabs/prysm/v6/testing/require"
	"github.com/OffchainLabs/prysm/v6/testing/util"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestValidateLightClientOptimisticUpdate_NilMessageOrTopic(t *testing.T) {
	ctx := context.Background()
	p := p2ptest.NewTestP2P(t)
	s := &Service{cfg: &config{p2p: p, initialSync: &mockSync.Sync{}}}

	_, err := s.validateLightClientOptimisticUpdate(ctx, "", nil)
	require.ErrorIs(t, err, errNilPubsubMessage)

	_, err = s.validateLightClientOptimisticUpdate(ctx, "", &pubsub.Message{Message: &pb.Message{}})
	require.ErrorIs(t, err, errNilPubsubMessage)

	emptyTopic := ""
	_, err = s.validateLightClientOptimisticUpdate(ctx, "", &pubsub.Message{Message: &pb.Message{
		Topic: &emptyTopic,
	}})
	require.ErrorIs(t, err, errNilPubsubMessage)
}

func TestValidateLightClientOptimisticUpdate_valid(t *testing.T) {
	ctx := context.Background()
	p := p2ptest.NewTestP2P(t)
	cfg := params.BeaconConfig()
	cfg.AltairForkEpoch = 1
	cfg.ForkVersionSchedule[[4]byte{1, 0, 0, 0}] = 1
	params.OverrideBeaconConfig(cfg)

	chainService := &mock.ChainService{Genesis: time.Unix(time.Now().Unix()-int64(2*uint64(params.BeaconConfig().SlotsPerEpoch)*params.BeaconConfig().SecondsPerSlot), 0)}
	s := &Service{cfg: &config{p2p: p, initialSync: &mockSync.Sync{}, clock: startup.NewClock(chainService.Genesis, chainService.ValidatorsRoot)}, lcStore: &lightClient.Store{}}

	l := util.NewTestLightClient(t, version.Altair)
	u, err := lightClient.NewLightClientOptimisticUpdateFromBeaconState(l.Ctx, l.State.Slot(), l.State, l.Block, l.AttestedState, l.AttestedBlock)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	_, err = p.Encoding().EncodeGossip(buf, u)
	require.NoError(t, err)

	topic := p2p.LightClientOptimisticUpdateTopicFormat
	digest, err := s.currentForkDigest()
	require.NoError(t, err)
	topic = s.addDigestToTopic(topic, digest)

	r, err := s.validateLightClientOptimisticUpdate(ctx, "", &pubsub.Message{
		Message: &pb.Message{
			Data:  buf.Bytes(),
			Topic: &topic,
		}})
	require.NoError(t, err)
	require.Equal(t, r, pubsub.ValidationAccept)
}

func TestValidateLightClientOptimisticUpdate_tooSoon(t *testing.T) {
	ctx := context.Background()
	p := p2ptest.NewTestP2P(t)
	cfg := params.BeaconConfig()
	cfg.AltairForkEpoch = 1
	cfg.ForkVersionSchedule[[4]byte{1, 0, 0, 0}] = 1
	params.OverrideBeaconConfig(cfg)

	genesisTime := time.Unix(time.Now().Unix()-int64(uint64(params.BeaconConfig().SlotsPerEpoch)*params.BeaconConfig().SecondsPerSlot+2*params.BeaconConfig().SecondsPerSlot+params.BeaconConfig().SecondsPerSlot/params.BeaconConfig().IntervalsPerSlot), 0)
	chainService := &mock.ChainService{Genesis: genesisTime}
	s := &Service{cfg: &config{p2p: p, initialSync: &mockSync.Sync{}, clock: startup.NewClock(chainService.Genesis, chainService.ValidatorsRoot)}, lcStore: &lightClient.Store{}}

	l := util.NewTestLightClient(t, version.Altair)
	u, err := lightClient.NewLightClientOptimisticUpdateFromBeaconState(l.Ctx, l.State.Slot(), l.State, l.Block, l.AttestedState, l.AttestedBlock)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	_, err = p.Encoding().EncodeGossip(buf, u)
	require.NoError(t, err)

	topic := p2p.LightClientOptimisticUpdateTopicFormat
	digest, err := s.currentForkDigest()
	require.NoError(t, err)
	topic = s.addDigestToTopic(topic, digest)

	r, err := s.validateLightClientOptimisticUpdate(ctx, "", &pubsub.Message{
		Message: &pb.Message{
			Data:  buf.Bytes(),
			Topic: &topic,
		}})
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationAccept, r)
}

func TestValidateLightClientOptimisticUpdate(t *testing.T) {
	cfg := params.BeaconConfig()
	cfg.AltairForkEpoch = 1
	cfg.BellatrixForkEpoch = 2
	cfg.CapellaForkEpoch = 3
	cfg.DenebForkEpoch = 4
	cfg.ElectraForkEpoch = 5
	cfg.ForkVersionSchedule[[4]byte{1, 0, 0, 0}] = 1
	cfg.ForkVersionSchedule[[4]byte{2, 0, 0, 0}] = 2
	cfg.ForkVersionSchedule[[4]byte{3, 0, 0, 0}] = 3
	cfg.ForkVersionSchedule[[4]byte{4, 0, 0, 0}] = 4
	cfg.ForkVersionSchedule[[4]byte{5, 0, 0, 0}] = 5
	params.OverrideBeaconConfig(cfg)

	secondsPerSlot := int(params.BeaconConfig().SecondsPerSlot)
	slotIntervals := int(params.BeaconConfig().IntervalsPerSlot)
	slotsPerEpoch := int(params.BeaconConfig().SlotsPerEpoch)

	tests := []struct {
		name             string
		genesisDrift     int
		oldUpdateOptions []util.LightClientOption
		newUpdateOptions []util.LightClientOption
		expectedResult   pubsub.ValidationResult
		expectedErr      error
	}{
		{
			name:             "no previous update",
			oldUpdateOptions: nil,
			newUpdateOptions: []util.LightClientOption{},
			expectedResult:   pubsub.ValidationAccept,
		},
		{
			name:             "not enough time passed",
			genesisDrift:     -secondsPerSlot / slotIntervals,
			oldUpdateOptions: nil,
			newUpdateOptions: []util.LightClientOption{},
			expectedResult:   pubsub.ValidationIgnore,
		},
		{
			name:             "new update has no age advantage",
			oldUpdateOptions: []util.LightClientOption{},
			newUpdateOptions: []util.LightClientOption{},
			expectedResult:   pubsub.ValidationIgnore,
		},
		{
			name:             "new update is better - younger",
			genesisDrift:     secondsPerSlot,
			oldUpdateOptions: []util.LightClientOption{},
			newUpdateOptions: []util.LightClientOption{util.WithIncreasedAttestedSlot(1)},
			expectedResult:   pubsub.ValidationAccept,
		},
	}

	for _, test := range tests {
		for v := 1; v < 6; v++ {
			t.Run(test.name+"_"+version.String(v), func(t *testing.T) {
				ctx := context.Background()
				p := p2ptest.NewTestP2P(t)
				// drift back appropriate number of epochs based on fork + 2 slots for signature slot + time for gossip propagation + any extra drift
				genesisDrift := v*slotsPerEpoch*secondsPerSlot + 2*secondsPerSlot + secondsPerSlot/slotIntervals + test.genesisDrift
				chainService := &mock.ChainService{Genesis: time.Unix(time.Now().Unix()-int64(genesisDrift), 0)}
				s := &Service{cfg: &config{p2p: p, initialSync: &mockSync.Sync{}, clock: startup.NewClock(chainService.Genesis, chainService.ValidatorsRoot)}, lcStore: &lightClient.Store{}}

				var oldUpdate interfaces.LightClientOptimisticUpdate
				var err error
				if test.oldUpdateOptions != nil {
					l := util.NewTestLightClient(t, v, test.oldUpdateOptions...)
					oldUpdate, err = lightClient.NewLightClientOptimisticUpdateFromBeaconState(l.Ctx, l.State.Slot(), l.State, l.Block, l.AttestedState, l.AttestedBlock)
					require.NoError(t, err)

					s.lcStore.SetLastOptimisticUpdate(oldUpdate)
				}

				l := util.NewTestLightClient(t, v, test.newUpdateOptions...)
				newUpdate, err := lightClient.NewLightClientOptimisticUpdateFromBeaconState(l.Ctx, l.State.Slot(), l.State, l.Block, l.AttestedState, l.AttestedBlock)
				require.NoError(t, err)
				buf := new(bytes.Buffer)
				_, err = p.Encoding().EncodeGossip(buf, newUpdate)
				require.NoError(t, err)

				topic := p2p.LightClientOptimisticUpdateTopicFormat
				digest, err := s.currentForkDigest()
				require.NoError(t, err)
				topic = s.addDigestToTopic(topic, digest)

				r, err := s.validateLightClientOptimisticUpdate(ctx, "", &pubsub.Message{
					Message: &pb.Message{
						Data:  buf.Bytes(),
						Topic: &topic,
					}})
				if test.expectedErr != nil {
					require.ErrorIs(t, err, test.expectedErr)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.expectedResult, r)
				}
			})
		}
	}
}
