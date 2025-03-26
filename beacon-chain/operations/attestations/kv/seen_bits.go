package kv

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"strconv"

	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/io/file"
	ethpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1/attestation"
	log "github.com/sirupsen/logrus"
)

func (c *AttCaches) insertSeenBit(att ethpb.Att) error {
	id, err := attestation.NewId(att, attestation.Data)
	if err != nil {
		return errors.Wrap(err, "could not create attestation ID")
	}

	v, ok := c.seenAtt.Get(id.String())
	if ok {
		seenBits, ok := v.([]bitfield.Bitlist)
		if !ok {
			return errors.New("could not convert to bitlist type")
		}
		alreadyExists := false
		for _, bit := range seenBits {
			if contains, err := bit.Contains(att.GetAggregationBits()); err != nil {
				c.handleBitlistError(id, att, bit)
				return err
			} else if contains {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			seenBits = append(seenBits, att.GetAggregationBits())
		}
		c.seenAtt.Set(id.String(), seenBits, cache.DefaultExpiration /* one epoch */)
		return nil
	}

	c.seenAtt.Set(id.String(), []bitfield.Bitlist{att.GetAggregationBits()}, cache.DefaultExpiration /* one epoch */)
	return nil
}

func (c *AttCaches) handleBitlistError(id attestation.Id, att ethpb.Att, seenBitlist bitfield.Bitlist) {
	c.bitlistErrCountLock.Lock()
	if c.fcDumpLimit == 0 {
		c.bitlistErrCountLock.Unlock()
		return
	}
	c.fcDumpLimit = c.fcDumpLimit - 1
	count := c.bitlistErrCount[id]
	c.bitlistErrCount[id] = count + 1
	c.bitlistErrCountLock.Unlock()

	attFilename := path.Join(os.TempDir(), id.String(), "-att-", strconv.FormatUint(count, 10), ".json")
	dumpFilename := path.Join(os.TempDir(), id.String(), "-dump-", strconv.FormatUint(count, 10), ".json")
	log.Debugf(
		"Found attestations with different bitlists (%d and %d). Saving attestation JSON to %s and fork choice dump JSON to %s",
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

	dump, dumpErr := c.fc.ForkChoiceDump(context.Background())
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

func (c *AttCaches) hasSeenBit(att ethpb.Att) (bool, error) {
	id, err := attestation.NewId(att, attestation.Data)
	if err != nil {
		return false, errors.Wrap(err, "could not create attestation ID")
	}

	v, ok := c.seenAtt.Get(id.String())
	if ok {
		seenBits, ok := v.([]bitfield.Bitlist)
		if !ok {
			return false, errors.New("could not convert to bitlist type")
		}
		for _, bit := range seenBits {
			if contains, err := bit.Contains(att.GetAggregationBits()); err != nil {
				c.handleBitlistError(id, att, bit)
				return false, err
			} else if contains {
				return true, nil
			}
		}
	}
	return false, nil
}
