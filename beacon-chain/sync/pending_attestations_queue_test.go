package sync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/async/abool"
	mock "github.com/prysmaticlabs/prysm/v5/beacon-chain/blockchain/testing"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/feed"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/feed/operation"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/helpers"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/signing"
	dbtest "github.com/prysmaticlabs/prysm/v5/beacon-chain/db/testing"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/operations/attestations"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/peers"
	p2ptest "github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/testing"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/startup"
	lruwrpr "github.com/prysmaticlabs/prysm/v5/cache/lru"
	fieldparams "github.com/prysmaticlabs/prysm/v5/config/fieldparams"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v5/crypto/bls"
	"github.com/prysmaticlabs/prysm/v5/encoding/bytesutil"
	ethpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1/attestation"
	"github.com/prysmaticlabs/prysm/v5/testing/assert"
	"github.com/prysmaticlabs/prysm/v5/testing/require"
	"github.com/prysmaticlabs/prysm/v5/testing/util"
	prysmTime "github.com/prysmaticlabs/prysm/v5/time"
	logTest "github.com/sirupsen/logrus/hooks/test"
)

func TestProcessPendingAtts_NoBlockRequestBlock(t *testing.T) {
	hook := logTest.NewGlobal()
	db := dbtest.SetupDB(t)
	p1 := p2ptest.NewTestP2P(t)
	p2 := p2ptest.NewTestP2P(t)
	p1.Connect(p2)
	assert.Equal(t, 1, len(p1.BHost.Network().Peers()), "Expected peers to be connected")
	p1.Peers().Add(new(enr.Record), p2.PeerID(), nil, network.DirOutbound)
	p1.Peers().SetConnectionState(p2.PeerID(), peers.Connected)
	p1.Peers().SetChainState(p2.PeerID(), &ethpb.Status{})

	chain := &mock.ChainService{Genesis: prysmTime.Now(), FinalizedCheckPoint: &ethpb.Checkpoint{}}
	r := &Service{
		cfg:                  &config{p2p: p1, beaconDB: db, chain: chain, clock: startup.NewClock(chain.Genesis, chain.ValidatorsRoot)},
		blkRootToPendingAtts: make(map[[32]byte][]any),
		chainStarted:         abool.New(),
	}

	a := &ethpb.Attestation{Data: &ethpb.AttestationData{Target: &ethpb.Checkpoint{Root: make([]byte, 32)}}}
	r.blkRootToPendingAtts[[32]byte{'A'}] = []any{a}
	require.NoError(t, r.processPendingAtts(context.Background()))
	require.LogsContain(t, hook, "Requesting block by root")
}

func TestProcessPendingAtts_HasBlockSaveUnaggregatedAtt(t *testing.T) {
	hook := logTest.NewGlobal()
	db := dbtest.SetupDB(t)
	p1 := p2ptest.NewTestP2P(t)
	validators := uint64(256)

	beaconState, privKeys := util.DeterministicGenesisState(t, validators)

	sb := util.NewBeaconBlock()
	util.SaveBlock(t, context.Background(), db, sb)
	root, err := sb.Block.HashTreeRoot()
	require.NoError(t, err)

	aggBits := bitfield.NewBitlist(8)
	aggBits.SetBitAt(1, true)
	att := &ethpb.Attestation{
		Data: &ethpb.AttestationData{
			BeaconBlockRoot: root[:],
			Source:          &ethpb.Checkpoint{Epoch: 0, Root: bytesutil.PadTo([]byte("hello-world"), 32)},
			Target:          &ethpb.Checkpoint{Epoch: 0, Root: root[:]},
		},
		AggregationBits: aggBits,
	}

	committee, err := helpers.BeaconCommitteeFromState(context.Background(), beaconState, att.Data.Slot, att.Data.CommitteeIndex)
	assert.NoError(t, err)
	attestingIndices, err := attestation.AttestingIndices(att, committee)
	require.NoError(t, err)
	attesterDomain, err := signing.Domain(beaconState.Fork(), 0, params.BeaconConfig().DomainBeaconAttester, beaconState.GenesisValidatorsRoot())
	require.NoError(t, err)
	hashTreeRoot, err := signing.ComputeSigningRoot(att.Data, attesterDomain)
	assert.NoError(t, err)
	for _, i := range attestingIndices {
		att.Signature = privKeys[i].Sign(hashTreeRoot[:]).Marshal()
	}

	require.NoError(t, beaconState.SetGenesisTime(uint64(time.Now().Unix())))

	chain := &mock.ChainService{Genesis: time.Now(),
		State: beaconState,
		FinalizedCheckPoint: &ethpb.Checkpoint{
			Root:  att.Data.BeaconBlockRoot,
			Epoch: 0,
		},
	}

	done := make(chan *feed.Event, 1)
	defer close(done)
	opn := mock.NewEventFeedWrapper()
	sub := opn.Subscribe(done)
	defer sub.Unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	r := &Service{
		ctx: ctx,
		cfg: &config{
			p2p:                 p1,
			beaconDB:            db,
			chain:               chain,
			clock:               startup.NewClock(chain.Genesis, chain.ValidatorsRoot),
			attPool:             attestations.NewPool(),
			attestationNotifier: &mock.SimpleNotifier{Feed: opn},
		},
		blkRootToPendingAtts:             make(map[[32]byte][]any),
		seenUnAggregatedAttestationCache: lruwrpr.New(10),
		signatureChan:                    make(chan *signatureVerifier, verifierLimit),
	}
	go r.verifierRoutine()

	s, err := util.NewBeaconState()
	require.NoError(t, err)
	require.NoError(t, r.cfg.beaconDB.SaveState(context.Background(), s, root))

	r.blkRootToPendingAtts[root] = []any{att}
	require.NoError(t, r.processPendingAtts(context.Background()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case received := <-done:
				// make sure a single att was sent
				require.Equal(t, operation.UnaggregatedAttReceived, int(received.Type))
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	atts, err := r.cfg.attPool.UnaggregatedAttestations()
	require.NoError(t, err)
	assert.Equal(t, 1, len(atts), "Did not save unaggregated att")
	assert.DeepEqual(t, att, atts[0], "Incorrect saved att")
	assert.Equal(t, 0, len(r.cfg.attPool.AggregatedAttestations()), "Did save aggregated att")
	require.LogsContain(t, hook, "Verified and saved pending attestations to pool")
	wg.Wait()
	cancel()
}

func TestProcessPendingAtts_HasBlockSaveUnaggregatedAttElectra(t *testing.T) {
	hook := logTest.NewGlobal()
	db := dbtest.SetupDB(t)
	p1 := p2ptest.NewTestP2P(t)
	validators := uint64(256)

	beaconState, privKeys := util.DeterministicGenesisStateElectra(t, validators)

	sb := util.NewBeaconBlockElectra()
	util.SaveBlock(t, context.Background(), db, sb)
	root, err := sb.Block.HashTreeRoot()
	require.NoError(t, err)

	att := &ethpb.SingleAttestation{
		Data: &ethpb.AttestationData{
			BeaconBlockRoot: root[:],
			Source:          &ethpb.Checkpoint{Epoch: 0, Root: bytesutil.PadTo([]byte("hello-world"), 32)},
			Target:          &ethpb.Checkpoint{Epoch: 0, Root: root[:]},
		},
	}

	committee, err := helpers.BeaconCommitteeFromState(context.Background(), beaconState, att.Data.Slot, att.Data.CommitteeIndex)
	assert.NoError(t, err)
	att.AttesterIndex = committee[0]
	attesterDomain, err := signing.Domain(beaconState.Fork(), 0, params.BeaconConfig().DomainBeaconAttester, beaconState.GenesisValidatorsRoot())
	require.NoError(t, err)
	hashTreeRoot, err := signing.ComputeSigningRoot(att.Data, attesterDomain)
	assert.NoError(t, err)
	att.Signature = privKeys[committee[0]].Sign(hashTreeRoot[:]).Marshal()

	require.NoError(t, beaconState.SetGenesisTime(uint64(time.Now().Unix())))

	chain := &mock.ChainService{Genesis: time.Now(),
		State: beaconState,
		FinalizedCheckPoint: &ethpb.Checkpoint{
			Root:  att.Data.BeaconBlockRoot,
			Epoch: 0,
		},
	}
	done := make(chan *feed.Event, 1)
	defer close(done)
	opn := mock.NewEventFeedWrapper()
	sub := opn.Subscribe(done)
	defer sub.Unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	r := &Service{
		ctx: ctx,
		cfg: &config{
			p2p:                 p1,
			beaconDB:            db,
			chain:               chain,
			clock:               startup.NewClock(chain.Genesis, chain.ValidatorsRoot),
			attPool:             attestations.NewPool(),
			attestationNotifier: &mock.SimpleNotifier{Feed: opn},
		},
		blkRootToPendingAtts:             make(map[[32]byte][]any),
		seenUnAggregatedAttestationCache: lruwrpr.New(10),
		signatureChan:                    make(chan *signatureVerifier, verifierLimit),
	}
	go r.verifierRoutine()

	s, err := util.NewBeaconStateElectra()
	require.NoError(t, err)
	require.NoError(t, r.cfg.beaconDB.SaveState(context.Background(), s, root))

	r.blkRootToPendingAtts[root] = []any{att}
	require.NoError(t, r.processPendingAtts(context.Background()))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case received := <-done:
				// make sure a single att was sent
				require.Equal(t, operation.SingleAttReceived, int(received.Type))
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	atts, err := r.cfg.attPool.UnaggregatedAttestations()
	require.NoError(t, err)
	require.Equal(t, 1, len(atts), "Did not save unaggregated att")
	assert.DeepEqual(t, att.ToAttestationElectra(committee), atts[0], "Incorrect saved att")
	assert.Equal(t, 0, len(r.cfg.attPool.AggregatedAttestations()), "Did save aggregated att")
	require.LogsContain(t, hook, "Verified and saved pending attestations to pool")
	wg.Wait()
	cancel()
}

func TestProcessPendingAtts_NoBroadcastWithBadSignature(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	db := dbtest.SetupDB(t)
	p2p := p2ptest.NewTestP2P(t)
	st, privKeys := util.DeterministicGenesisState(t, 256)
	require.NoError(t, st.SetGenesisTime(uint64(time.Now().Unix())))
	b := util.NewBeaconBlock()
	r32, err := b.Block.HashTreeRoot()
	require.NoError(t, err)
	util.SaveBlock(t, context.Background(), db, b)
	require.NoError(t, db.SaveState(context.Background(), st, r32))

	chain := &mock.ChainService{
		State:   st,
		Genesis: prysmTime.Now(),
		DB:      db,
		FinalizedCheckPoint: &ethpb.Checkpoint{
			Root:  r32[:],
			Epoch: 0,
		},
	}

	s := &Service{
		ctx: ctx,
		cfg: &config{
			p2p:      p2p,
			beaconDB: db,
			chain:    chain,
			clock:    startup.NewClock(chain.Genesis, chain.ValidatorsRoot),
			attPool:  attestations.NewPool(),
		},
		blkRootToPendingAtts:           make(map[[32]byte][]any),
		signatureChan:                  make(chan *signatureVerifier, verifierLimit),
		seenAggregatedAttestationCache: lruwrpr.New(10),
	}
	go s.verifierRoutine()

	committee, err := helpers.BeaconCommitteeFromState(context.Background(), st, 0, 0)
	assert.NoError(t, err)
	// Arbitrary aggregator index for testing purposes.
	aggregatorIndex := committee[0]

	priv, err := bls.RandKey()
	require.NoError(t, err)
	aggBits := bitfield.NewBitlist(8)
	aggBits.SetBitAt(1, true)

	a := &ethpb.AggregateAttestationAndProof{
		Aggregate: &ethpb.Attestation{
			Signature:       priv.Sign([]byte("foo")).Marshal(),
			AggregationBits: aggBits,
			Data:            util.HydrateAttestationData(&ethpb.AttestationData{}),
		},
		AggregatorIndex: aggregatorIndex,
		SelectionProof:  make([]byte, fieldparams.BLSSignatureLength),
	}

	s.blkRootToPendingAtts[r32] = []any{&ethpb.SignedAggregateAttestationAndProof{Message: a, Signature: make([]byte, fieldparams.BLSSignatureLength)}}
	require.NoError(t, s.processPendingAtts(context.Background()))

	assert.Equal(t, false, p2p.BroadcastCalled.Load(), "Broadcasted bad aggregate")

	// Clear pool.
	err = s.cfg.attPool.DeleteUnaggregatedAttestation(a.Aggregate)
	require.NoError(t, err)

	att := &ethpb.Attestation{
		Data: &ethpb.AttestationData{
			BeaconBlockRoot: r32[:],
			Source:          &ethpb.Checkpoint{Epoch: 0, Root: bytesutil.PadTo([]byte("hello-world"), 32)},
			Target:          &ethpb.Checkpoint{Epoch: 0, Root: r32[:]},
		},
		AggregationBits: aggBits,
	}

	attestingIndices, err := attestation.AttestingIndices(att, committee)
	require.NoError(t, err)
	attesterDomain, err := signing.Domain(st.Fork(), 0, params.BeaconConfig().DomainBeaconAttester, st.GenesisValidatorsRoot())
	require.NoError(t, err)
	hashTreeRoot, err := signing.ComputeSigningRoot(att.Data, attesterDomain)
	assert.NoError(t, err)
	for _, i := range attestingIndices {
		att.Signature = privKeys[i].Sign(hashTreeRoot[:]).Marshal()
	}

	sszSlot := primitives.SSZUint64(att.Data.Slot)
	sig, err := signing.ComputeDomainAndSign(st, 0, &sszSlot, params.BeaconConfig().DomainSelectionProof, privKeys[aggregatorIndex])
	require.NoError(t, err)
	aggregateAndProof := &ethpb.AggregateAttestationAndProof{
		SelectionProof:  sig,
		Aggregate:       att,
		AggregatorIndex: aggregatorIndex,
	}
	aggreSig, err := signing.ComputeDomainAndSign(st, 0, aggregateAndProof, params.BeaconConfig().DomainAggregateAndProof, privKeys[aggregatorIndex])
	require.NoError(t, err)

	s.blkRootToPendingAtts[r32] = []any{&ethpb.SignedAggregateAttestationAndProof{Message: aggregateAndProof, Signature: aggreSig}}
	require.NoError(t, s.processPendingAtts(context.Background()))

	assert.Equal(t, true, p2p.BroadcastCalled.Load(), "The good aggregate was not broadcasted")

	cancel()
}

func TestProcessPendingAtts_HasBlockSaveAggregatedAtt(t *testing.T) {
	hook := logTest.NewGlobal()
	db := dbtest.SetupDB(t)
	p1 := p2ptest.NewTestP2P(t)
	validators := uint64(256)

	beaconState, privKeys := util.DeterministicGenesisState(t, validators)

	sb := util.NewBeaconBlock()
	util.SaveBlock(t, context.Background(), db, sb)
	root, err := sb.Block.HashTreeRoot()
	require.NoError(t, err)

	aggBits := bitfield.NewBitlist(validators / uint64(params.BeaconConfig().SlotsPerEpoch))
	aggBits.SetBitAt(0, true)
	aggBits.SetBitAt(1, true)
	att := &ethpb.Attestation{
		Data: &ethpb.AttestationData{
			BeaconBlockRoot: root[:],
			Source:          &ethpb.Checkpoint{Epoch: 0, Root: bytesutil.PadTo([]byte("hello-world"), 32)},
			Target:          &ethpb.Checkpoint{Epoch: 0, Root: root[:]},
		},
		AggregationBits: aggBits,
	}

	committee, err := helpers.BeaconCommitteeFromState(context.Background(), beaconState, att.Data.Slot, att.Data.CommitteeIndex)
	assert.NoError(t, err)
	attestingIndices, err := attestation.AttestingIndices(att, committee)
	require.NoError(t, err)
	attesterDomain, err := signing.Domain(beaconState.Fork(), 0, params.BeaconConfig().DomainBeaconAttester, beaconState.GenesisValidatorsRoot())
	require.NoError(t, err)
	hashTreeRoot, err := signing.ComputeSigningRoot(att.Data, attesterDomain)
	assert.NoError(t, err)
	sigs := make([]bls.Signature, len(attestingIndices))
	for i, indice := range attestingIndices {
		sig := privKeys[indice].Sign(hashTreeRoot[:])
		sigs[i] = sig
	}
	att.Signature = bls.AggregateSignatures(sigs).Marshal()

	// Arbitrary aggregator index for testing purposes.
	aggregatorIndex := committee[0]
	sszUint := primitives.SSZUint64(att.Data.Slot)
	sig, err := signing.ComputeDomainAndSign(beaconState, 0, &sszUint, params.BeaconConfig().DomainSelectionProof, privKeys[aggregatorIndex])
	require.NoError(t, err)
	aggregateAndProof := &ethpb.AggregateAttestationAndProof{
		SelectionProof:  sig,
		Aggregate:       att,
		AggregatorIndex: aggregatorIndex,
	}
	aggreSig, err := signing.ComputeDomainAndSign(beaconState, 0, aggregateAndProof, params.BeaconConfig().DomainAggregateAndProof, privKeys[aggregatorIndex])
	require.NoError(t, err)

	require.NoError(t, beaconState.SetGenesisTime(uint64(time.Now().Unix())))

	chain := &mock.ChainService{Genesis: time.Now(),
		DB:    db,
		State: beaconState,
		FinalizedCheckPoint: &ethpb.Checkpoint{
			Root:  aggregateAndProof.Aggregate.Data.BeaconBlockRoot,
			Epoch: 0,
		}}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Service{
		ctx: ctx,
		cfg: &config{
			p2p:      p1,
			beaconDB: db,
			chain:    chain,
			clock:    startup.NewClock(chain.Genesis, chain.ValidatorsRoot),
			attPool:  attestations.NewPool(),
		},
		blkRootToPendingAtts:           make(map[[32]byte][]any),
		seenAggregatedAttestationCache: lruwrpr.New(10),
		signatureChan:                  make(chan *signatureVerifier, verifierLimit),
	}
	go r.verifierRoutine()
	s, err := util.NewBeaconState()
	require.NoError(t, err)
	require.NoError(t, r.cfg.beaconDB.SaveState(context.Background(), s, root))

	r.blkRootToPendingAtts[root] = []any{&ethpb.SignedAggregateAttestationAndProof{Message: aggregateAndProof, Signature: aggreSig}}
	require.NoError(t, r.processPendingAtts(context.Background()))

	assert.Equal(t, 1, len(r.cfg.attPool.AggregatedAttestations()), "Did not save aggregated att")
	assert.DeepEqual(t, att, r.cfg.attPool.AggregatedAttestations()[0], "Incorrect saved att")
	atts, err := r.cfg.attPool.UnaggregatedAttestations()
	require.NoError(t, err)
	assert.Equal(t, 0, len(atts), "Did save aggregated att")
	require.LogsContain(t, hook, "Verified and saved pending attestations to pool")
	cancel()
}

func TestProcessPendingAtts_HasBlockSaveAggregatedAttElectra(t *testing.T) {
	hook := logTest.NewGlobal()
	db := dbtest.SetupDB(t)
	p1 := p2ptest.NewTestP2P(t)
	validators := uint64(256)

	beaconState, privKeys := util.DeterministicGenesisState(t, validators)

	sb := util.NewBeaconBlock()
	util.SaveBlock(t, context.Background(), db, sb)
	root, err := sb.Block.HashTreeRoot()
	require.NoError(t, err)

	committeeBits := primitives.NewAttestationCommitteeBits()
	committeeBits.SetBitAt(0, true)
	aggBits := bitfield.NewBitlist(validators / uint64(params.BeaconConfig().SlotsPerEpoch))
	aggBits.SetBitAt(0, true)
	aggBits.SetBitAt(1, true)
	att := &ethpb.AttestationElectra{
		Data: &ethpb.AttestationData{
			BeaconBlockRoot: root[:],
			Source:          &ethpb.Checkpoint{Epoch: 0, Root: bytesutil.PadTo([]byte("hello-world"), 32)},
			Target:          &ethpb.Checkpoint{Epoch: 0, Root: root[:]},
		},
		CommitteeBits:   committeeBits,
		AggregationBits: aggBits,
	}

	committee, err := helpers.BeaconCommitteeFromState(context.Background(), beaconState, att.Data.Slot, att.GetCommitteeIndex())
	assert.NoError(t, err)
	attestingIndices, err := attestation.AttestingIndices(att, committee)
	require.NoError(t, err)
	attesterDomain, err := signing.Domain(beaconState.Fork(), 0, params.BeaconConfig().DomainBeaconAttester, beaconState.GenesisValidatorsRoot())
	require.NoError(t, err)
	hashTreeRoot, err := signing.ComputeSigningRoot(att.Data, attesterDomain)
	assert.NoError(t, err)
	sigs := make([]bls.Signature, len(attestingIndices))
	for i, indice := range attestingIndices {
		sig := privKeys[indice].Sign(hashTreeRoot[:])
		sigs[i] = sig
	}
	att.Signature = bls.AggregateSignatures(sigs).Marshal()

	// Arbitrary aggregator index for testing purposes.
	aggregatorIndex := committee[0]
	sszUint := primitives.SSZUint64(att.Data.Slot)
	sig, err := signing.ComputeDomainAndSign(beaconState, 0, &sszUint, params.BeaconConfig().DomainSelectionProof, privKeys[aggregatorIndex])
	require.NoError(t, err)
	aggregateAndProof := &ethpb.AggregateAttestationAndProofElectra{
		SelectionProof:  sig,
		Aggregate:       att,
		AggregatorIndex: aggregatorIndex,
	}
	aggreSig, err := signing.ComputeDomainAndSign(beaconState, 0, aggregateAndProof, params.BeaconConfig().DomainAggregateAndProof, privKeys[aggregatorIndex])
	require.NoError(t, err)

	require.NoError(t, beaconState.SetGenesisTime(uint64(time.Now().Unix())))

	chain := &mock.ChainService{Genesis: time.Now(),
		DB:    db,
		State: beaconState,
		FinalizedCheckPoint: &ethpb.Checkpoint{
			Root:  aggregateAndProof.Aggregate.Data.BeaconBlockRoot,
			Epoch: 0,
		}}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Service{
		ctx: ctx,
		cfg: &config{
			p2p:      p1,
			beaconDB: db,
			chain:    chain,
			clock:    startup.NewClock(chain.Genesis, chain.ValidatorsRoot),
			attPool:  attestations.NewPool(),
		},
		blkRootToPendingAtts:           make(map[[32]byte][]any),
		seenAggregatedAttestationCache: lruwrpr.New(10),
		signatureChan:                  make(chan *signatureVerifier, verifierLimit),
	}
	go r.verifierRoutine()
	s, err := util.NewBeaconState()
	require.NoError(t, err)
	require.NoError(t, r.cfg.beaconDB.SaveState(context.Background(), s, root))

	r.blkRootToPendingAtts[root] = []any{&ethpb.SignedAggregateAttestationAndProofElectra{Message: aggregateAndProof, Signature: aggreSig}}
	require.NoError(t, r.processPendingAtts(context.Background()))

	assert.Equal(t, 1, len(r.cfg.attPool.AggregatedAttestations()), "Did not save aggregated att")
	assert.DeepEqual(t, att, r.cfg.attPool.AggregatedAttestations()[0], "Incorrect saved att")
	atts, err := r.cfg.attPool.UnaggregatedAttestations()
	require.NoError(t, err)
	assert.Equal(t, 0, len(atts), "Did save aggregated att")
	require.LogsContain(t, hook, "Verified and saved pending attestations to pool")
	cancel()
}

func TestValidatePendingAtts_CanPruneOldAtts(t *testing.T) {
	s := &Service{
		blkRootToPendingAtts: make(map[[32]byte][]any),
	}

	// 100 Attestations per block root.
	r1 := [32]byte{'A'}
	r2 := [32]byte{'B'}
	r3 := [32]byte{'C'}

	for i := primitives.Slot(0); i < 100; i++ {
		s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: i, BeaconBlockRoot: r1[:]}})
		s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: i, BeaconBlockRoot: r2[:]}})
		s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: i, BeaconBlockRoot: r3[:]}})
	}

	assert.Equal(t, 100, len(s.blkRootToPendingAtts[r1]), "Did not save pending atts")
	assert.Equal(t, 100, len(s.blkRootToPendingAtts[r2]), "Did not save pending atts")
	assert.Equal(t, 100, len(s.blkRootToPendingAtts[r3]), "Did not save pending atts")

	// Set current slot to 50, it should prune 19 attestations. (50 - 31)
	s.validatePendingAtts(context.Background(), 50)
	assert.Equal(t, 81, len(s.blkRootToPendingAtts[r1]), "Did not delete pending atts")
	assert.Equal(t, 81, len(s.blkRootToPendingAtts[r2]), "Did not delete pending atts")
	assert.Equal(t, 81, len(s.blkRootToPendingAtts[r3]), "Did not delete pending atts")

	// Set current slot to 100 + slot_duration, it should prune all the attestations.
	s.validatePendingAtts(context.Background(), 100+params.BeaconConfig().SlotsPerEpoch)
	assert.Equal(t, 0, len(s.blkRootToPendingAtts[r1]), "Did not delete pending atts")
	assert.Equal(t, 0, len(s.blkRootToPendingAtts[r2]), "Did not delete pending atts")
	assert.Equal(t, 0, len(s.blkRootToPendingAtts[r3]), "Did not delete pending atts")

	// Verify the keys are deleted.
	assert.Equal(t, 0, len(s.blkRootToPendingAtts), "Did not delete block keys")
}

func TestValidatePendingAtts_NoDuplicatingAtts(t *testing.T) {
	s := &Service{
		blkRootToPendingAtts: make(map[[32]byte][]any),
	}

	r1 := [32]byte{'A'}
	r2 := [32]byte{'B'}
	s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, BeaconBlockRoot: r1[:]}})
	s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 2, BeaconBlockRoot: r2[:]}})
	s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 2, BeaconBlockRoot: r2[:]}})

	assert.Equal(t, 1, len(s.blkRootToPendingAtts[r1]), "Did not save pending atts")
	assert.Equal(t, 1, len(s.blkRootToPendingAtts[r2]), "Did not save pending atts")
}

func TestSavePendingAtts_BeyondLimit(t *testing.T) {
	s := &Service{
		blkRootToPendingAtts: make(map[[32]byte][]any),
	}

	for i := 0; i < pendingAttsLimit; i++ {
		s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, BeaconBlockRoot: bytesutil.Bytes32(uint64(i))}})
	}
	r1 := [32]byte(bytesutil.Bytes32(0))
	r2 := [32]byte(bytesutil.Bytes32(uint64(pendingAttsLimit) - 1))

	assert.Equal(t, 1, len(s.blkRootToPendingAtts[r1]), "Did not save pending atts")
	assert.Equal(t, 1, len(s.blkRootToPendingAtts[r2]), "Did not save pending atts")

	for i := pendingAttsLimit; i < pendingAttsLimit+20; i++ {
		s.savePendingAtt(&ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, BeaconBlockRoot: bytesutil.Bytes32(uint64(i))}})
	}

	r1 = [32]byte(bytesutil.Bytes32(uint64(pendingAttsLimit)))
	r2 = [32]byte(bytesutil.Bytes32(uint64(pendingAttsLimit) + 10))

	assert.Equal(t, 0, len(s.blkRootToPendingAtts[r1]), "Saved pending atts")
	assert.Equal(t, 0, len(s.blkRootToPendingAtts[r2]), "Saved pending atts")
}

func Test_pendingAggregatesAreEqual(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		a := &ethpb.SignedAggregateAttestationAndProof{Message: &ethpb.AggregateAttestationAndProof{AggregatorIndex: 1}}
		b := &ethpb.SignedAggregateAttestationAndProof{Message: &ethpb.AggregateAttestationAndProof{AggregatorIndex: 1}}
		assert.Equal(t, true, pendingAggregatesAreEqual(a, b))
	})
	t.Run("different version", func(t *testing.T) {
		a := &ethpb.SignedAggregateAttestationAndProof{Message: &ethpb.AggregateAttestationAndProof{AggregatorIndex: 1}}
		b := &ethpb.SignedAggregateAttestationAndProofElectra{Message: &ethpb.AggregateAttestationAndProofElectra{AggregatorIndex: 1}}
		assert.Equal(t, false, pendingAggregatesAreEqual(a, b))
	})
	t.Run("different aggregator index", func(t *testing.T) {
		a := &ethpb.SignedAggregateAttestationAndProof{Message: &ethpb.AggregateAttestationAndProof{AggregatorIndex: 1}}
		b := &ethpb.SignedAggregateAttestationAndProof{Message: &ethpb.AggregateAttestationAndProof{AggregatorIndex: 2}}
		assert.Equal(t, false, pendingAggregatesAreEqual(a, b))
	})
}

func Test_pendingAttsAreEqual(t *testing.T) {
	t.Run("equal Phase0", func(t *testing.T) {
		a := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		b := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		assert.Equal(t, true, pendingAttsAreEqual(a, b))
	})
	t.Run("equal Electra", func(t *testing.T) {
		a := &ethpb.SingleAttestation{Data: &ethpb.AttestationData{Slot: 1}, AttesterIndex: 1}
		b := &ethpb.SingleAttestation{Data: &ethpb.AttestationData{Slot: 1}, AttesterIndex: 1}
		assert.Equal(t, true, pendingAttsAreEqual(a, b))
	})
	t.Run("different version", func(t *testing.T) {
		a := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		b := &ethpb.SingleAttestation{Data: &ethpb.AttestationData{Slot: 1}, AttesterIndex: 1}
		assert.Equal(t, false, pendingAttsAreEqual(a, b))
	})
	t.Run("different slot", func(t *testing.T) {
		a := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		b := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 2, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		assert.Equal(t, false, pendingAttsAreEqual(a, b))
	})
	t.Run("different committee index", func(t *testing.T) {
		a := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		b := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 2}, AggregationBits: bitfield.Bitlist{0b1111}}
		assert.Equal(t, false, pendingAttsAreEqual(a, b))
	})
	t.Run("different aggregation bits", func(t *testing.T) {
		a := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1111}}
		b := &ethpb.Attestation{Data: &ethpb.AttestationData{Slot: 1, CommitteeIndex: 1}, AggregationBits: bitfield.Bitlist{0b1000}}
		assert.Equal(t, false, pendingAttsAreEqual(a, b))
	})
	t.Run("different attester index", func(t *testing.T) {
		a := &ethpb.SingleAttestation{Data: &ethpb.AttestationData{Slot: 1}, AttesterIndex: 1}
		b := &ethpb.SingleAttestation{Data: &ethpb.AttestationData{Slot: 1}, AttesterIndex: 2}
		assert.Equal(t, false, pendingAttsAreEqual(a, b))
	})
}

func Test_SeenCommitteeIndicesSlot(t *testing.T) {
	t.Run("phase 0 success", func(t *testing.T) {
		s := &Service{
			seenUnAggregatedAttestationCache: lruwrpr.New(1),
		}
		data := &ethpb.AttestationData{Slot: 1, CommitteeIndex: 44}
		att := &ethpb.Attestation{
			AggregationBits: bitfield.Bitlist{0x01},
			Data:            data,
		}
		s.setSeenCommitteeIndicesSlot(data.Slot, att.GetCommitteeIndex(), att.GetAggregationBits())
		b := append(bytesutil.Bytes32(uint64(1)), bytesutil.Bytes32(uint64(44))...)
		b = append(b, bytesutil.SafeCopyBytes(att.GetAggregationBits())...)
		_, ok := s.seenUnAggregatedAttestationCache.Get(string(b))
		require.Equal(t, true, ok)
	})
	t.Run("electra success", func(t *testing.T) {
		s := &Service{
			seenUnAggregatedAttestationCache: lruwrpr.New(1),
		}
		// committee index is 0 post electra for attestation electra
		data := &ethpb.AttestationData{Slot: 1, CommitteeIndex: 0}
		cb := primitives.NewAttestationCommitteeBits()
		cb.SetBitAt(uint64(63), true)
		att := &ethpb.AttestationElectra{
			AggregationBits: bitfield.Bitlist{0x01},
			Data:            data,
			CommitteeBits:   cb,
		}
		ci := att.GetCommitteeIndex()
		s.setSeenCommitteeIndicesSlot(data.Slot, ci, att.GetAggregationBits())
		b := append(bytesutil.Bytes32(uint64(1)), bytesutil.Bytes32(uint64(63))...)
		b = append(b, bytesutil.SafeCopyBytes(att.GetAggregationBits())...)
		_, ok := s.seenUnAggregatedAttestationCache.Get(string(b))
		require.Equal(t, true, ok)
	})
}
