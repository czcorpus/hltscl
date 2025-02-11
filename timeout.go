package hltscl

import (
	"context"
	"sync"
	"time"
)

const (
	defaultBackoffFactor    = 1.5
	defaultRecoveryFactor   = 1.2
	defaultRecoveryInterval = 10 * time.Second
)

type timeoutController struct {
	mu  sync.RWMutex
	ctx context.Context

	// Configuration
	baseTimeout    time.Duration
	minTimeout     time.Duration
	backoffFactor  float64
	recoveryFactor float64

	// Current state
	currentTimeout time.Duration
	lastSuccess    time.Time

	// Recovery settings
	recoveryInterval time.Duration
}

func newTimeoutController(
	ctx context.Context,
	baseTimeout,
	minTimeout time.Duration,
) *timeoutController {
	tc := &timeoutController{
		baseTimeout:      baseTimeout,
		minTimeout:       minTimeout,
		currentTimeout:   baseTimeout,
		backoffFactor:    defaultBackoffFactor,  // Will reduce timeout by multiplying by 1/x
		recoveryFactor:   defaultRecoveryFactor, // Will increase timeout by multiplying by x
		recoveryInterval: defaultRecoveryInterval,
		lastSuccess:      time.Now(),
		ctx:              ctx,
	}
	go func() {
		ticker := time.NewTicker(tc.recoveryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				tc.attemptRecovery()
			case <-tc.ctx.Done():
				return
			}
		}
	}()

	return tc
}

func (tc *timeoutController) attemptRecovery() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// If we haven't had a success in a while, don't recover
	if time.Since(tc.lastSuccess) > tc.recoveryInterval*2 {
		return
	}
	tc.currentTimeout = min(time.Duration(float64(tc.currentTimeout)*tc.recoveryFactor), tc.baseTimeout)
}

func (tc *timeoutController) GetTimeout() time.Duration {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.currentTimeout
}

func (tc *timeoutController) reportSuccess() {
	tc.mu.Lock()
	tc.lastSuccess = time.Now()
	tc.mu.Unlock()
}

func (tc *timeoutController) ReportFailure() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.currentTimeout = max(time.Duration(float64(tc.currentTimeout)/tc.backoffFactor), tc.minTimeout)
}
