// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistence

import (
	"sync"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	// Throttler is used to control rate of requests to ensure the system does not
	// overwhelm persistence with requests
	Throttler interface {
		ShouldThrottleRequest() bool
		ReportError(err error)
	}

	throttler struct {
		rateLimiter            common.TokenBucket
		initialBackoffInterval time.Duration
		maxBackoffInterval     time.Duration

		sync.RWMutex
		delay                bool
		lastRequestTimestamp time.Time
		backoff              time.Duration
	}

	// used for testing
	noopThrottler struct {
	}
)

// NewThrottler creates a new persistence throttler
func NewThrottler(rps int, initialBackoffInterval time.Duration, maxBackoffInterval time.Duration) Throttler {
	return &throttler{
		rateLimiter:            common.NewTokenBucket(rps, common.NewRealTimeSource()),
		initialBackoffInterval: initialBackoffInterval,
		maxBackoffInterval:     maxBackoffInterval,
	}
}

func (t *throttler) ShouldThrottleRequest() bool {
	t.RLock()
	defer t.RUnlock()

	if t.delay && t.lastRequestTimestamp.Add(t.backoff).After(time.Now()) {
		return true
	}

	if ok, _ := t.rateLimiter.TryConsume(1); !ok {
		return true
	}
	return false
}

func (t *throttler) ReportError(err error) {
	t.lastRequestTimestamp = time.Now()
	switch err.(type) {
	case *shared.InternalServiceError, *shared.ServiceBusyError, *TimeoutError:
		{
			t.delay = true
			t.backoff *= 2
			if t.backoff > t.maxBackoffInterval {
				t.backoff = t.maxBackoffInterval
			}
		}
	default:
		{
			t.delay = false
			t.backoff = t.initialBackoffInterval
		}
	}
}

func newNoopThrottler() Throttler {
	return &noopThrottler{}
}

func (t *noopThrottler) ShouldThrottleRequest() bool {
	return false
}

func (t *noopThrottler) ReportError(err error) {

}
