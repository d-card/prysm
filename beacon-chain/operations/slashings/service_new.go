package slashings

import (
	"context"
	"time"

	"github.com/prysmaticlabs/prysm/v5/beacon-chain/startup"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v5/time/slots"
)

// WithElectraTimer includes functional options for the blockchain service related to CLI flags.
func WithElectraTimer(cw startup.ClockWaiter, currentSlotFn func() primitives.Slot) Option {
	return func(p *PoolService) error {
		p.runElectraTimer = true
		p.cw = cw
		p.currentSlotFn = currentSlotFn
		return nil
	}
}

// NewPoolService returns a service that manages the Pool.
func NewPoolService(ctx context.Context, pool PoolManager, opts ...Option) *PoolService {
	ctx, cancel := context.WithCancel(ctx)
	p := &PoolService{
		ctx:         ctx,
		cancel:      cancel,
		poolManager: pool,
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil
		}
	}

	return p
}

// Start the slashing pool service.
func (p *PoolService) Start() {
	go p.run()
}

func (p *PoolService) run() {
	if !p.runElectraTimer {
		return
	}

	// if Electra has not been scheduled return
	if params.BeaconConfig().ElectraForkEpoch == params.BeaconConfig().FarFutureEpoch {
		return
	}

	// If run() is executed after the transition to Electra has already happened,
	// there is nothing to convert because the slashing pool is empty at startup.
	if slots.ToEpoch(p.currentSlotFn()) >= params.BeaconConfig().ElectraForkEpoch {
		return
	}

	p.waitForChainInitialization()

	ticker := time.NewTicker(time.Duration(params.BeaconConfig().SecondsPerSlot) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			log.Warning("Context cancelled, ConvertToElectra aborted")
			return
		case <-ticker.C:
			if slots.ToEpoch(p.currentSlotFn()) >= params.BeaconConfig().ElectraForkEpoch {
				log.Info("Converting Phase0 slashings to Electra slashings")
				p.poolManager.ConvertToElectra()
				return
			}
		}
	}
}

func (p *PoolService) waitForChainInitialization() {
	clock, err := p.cw.WaitForClock(p.ctx)
	if err != nil {
		log.WithError(err).Error("Could not receive chain start notification")
	}
	p.clock = clock
	log.WithField("genesisTime", clock.GenesisTime()).Info(
		"Slashing pool service received chain initialization event",
	)
}

// Stop the slashing pool service.
func (p *PoolService) Stop() error {
	p.cancel()
	return nil
}

// Status of the slashing pool service.
func (p *PoolService) Status() error {
	return nil
}
