package lightclient

import (
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/blockchain"
	lightclient "github.com/prysmaticlabs/prysm/v5/beacon-chain/core/light-client"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/db"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/rpc/lookup"
)

type Server struct {
	Blocker          lookup.Blocker
	Stater           lookup.Stater
	HeadFetcher      blockchain.HeadFetcher
	ChainInfoFetcher blockchain.ChainInfoFetcher
	BeaconDB         db.HeadAccessDatabase
	LCStore          *lightclient.Store
}
