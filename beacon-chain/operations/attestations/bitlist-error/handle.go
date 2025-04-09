package bitlist_error

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/forkchoice"
	"github.com/prysmaticlabs/prysm/v5/io/file"
	ethpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1/attestation"
	log "github.com/sirupsen/logrus"
)

type AttAndSeenBitset struct {
	Att      ethpb.Att
	SeenBits bitfield.Bitlist
}

type BitlistErrorHandler struct {
	Fc                  forkchoice.ForkChoicer
	FcDumpLimit         uint64
	BitlistErrCountLock sync.RWMutex
	BitlistErrCount     map[attestation.Id]uint64
}

func (h *BitlistErrorHandler) Handle(id attestation.Id, att ethpb.Att, seenBitlist bitfield.Bitlist) {
	h.BitlistErrCountLock.Lock()
	if h.FcDumpLimit == 0 {
		h.BitlistErrCountLock.Unlock()
		return
	}
	h.FcDumpLimit = h.FcDumpLimit - 1
	count := h.BitlistErrCount[id]
	h.BitlistErrCount[id] = count + 1
	h.BitlistErrCountLock.Unlock()

	attFilename := os.TempDir() + hexutil.Encode([]byte(id.String())) + "-atts-" + strconv.FormatUint(count, 10) + ".json"
	dumpFilename := os.TempDir() + hexutil.Encode([]byte(id.String())) + "-dump-" + strconv.FormatUint(count, 10) + ".json"
	log.Debugf(
		"Found attestations with different bitlist lengths (%d and %d). Saving attestation JSON to %s and fork choice dump JSON to %s",
		len(att.GetAggregationBits()),
		len(seenBitlist),
		attFilename,
		dumpFilename,
	)

	jsonAtt, marshalErr := json.Marshal(att)
	if marshalErr != nil {
		log.WithError(marshalErr).Debug("Could not marshal attestation into JSON")
	}
	if writeErr := file.WriteFile(attFilename, jsonAtt); writeErr != nil {
		log.WithError(writeErr).Debug("Could not write attestation JSON to file")
	}

	dump, dumpErr := h.Fc.ForkChoiceDump(context.Background())
	if dumpErr != nil {
		log.WithError(dumpErr).Debug("Could not get fork choice dump")
	}
	jsonDump, marshalErr := json.Marshal(dump)
	if marshalErr != nil {
		log.WithError(marshalErr).Debug("Could not marshal fork choice dump into JSON")
	}
	if writeErr := file.WriteFile(dumpFilename, jsonDump); writeErr != nil {
		log.WithError(writeErr).Debug("Could not write fork choice dump JSON to file")
	}
}
