package golocker

import (
	"context"
	"sync"
	"time"

	"github.com/pborman/uuid"
)

// locker is a distributed mutex that is managed by the golocker Client.
//
// It should never be copied like that of sync.Mutex. Rather create a new locker using the golocker Client.
type locker struct {
	ctx                   context.Context
	distributedIdentifier string // used to sync mutexes across goroutines and hosts
	instanceIdentifier    string // used for identifying and discriminating between locker instances

	mu      sync.Mutex
	leaseID int64
	hasLock bool

	locked        chan int64 // lease id returned once lock acquired
	lockingFailed chan time.Duration
	unlocked      chan time.Duration
	unlockFailed  chan time.Duration

	autoExpireLockAfter time.Duration

	requestLock   chan<- *locker // parent locker's lock request channel
	requestUnlock chan<- *locker // parent locker's release request channel
}

func newLocker(ctx context.Context, distributedIdentifier string, autoExpireLockAfter time.Duration,
	lockRequests chan *locker, unlockRequests chan *locker) *locker {
	return &locker{
		ctx:                   ctx,
		distributedIdentifier: distributedIdentifier,
		instanceIdentifier:    uuid.New(),
		locked:                make(chan int64),
		lockingFailed:         make(chan time.Duration),
		unlocked:              make(chan time.Duration),
		unlockFailed:          make(chan time.Duration),
		autoExpireLockAfter:   autoExpireLockAfter,
		requestLock:           lockRequests,
		requestUnlock:         unlockRequests,
	}
}

// Lock is a blocking call until the lock has been acquired.
func (l *locker) Lock() {
	// pass request to locker
	l.requestLock <- l

	// wait for feedback from locker
	for {
		select {
		case <-l.ctx.Done():
			// context finished, exit cleanly
			return
		case backoff := <-l.lockingFailed:
			// backoff and retry
			time.Sleep(backoff)
			l.requestLock <- l
		case leaseID := <-l.locked:
			// lock acquired, end blocking pattern
			l.mu.Lock()
			l.leaseID = leaseID
			l.hasLock = true
			l.mu.Unlock()
			return
		default:
			continue
		}
	}
}

// Unlock unlocks the distributed locker
func (l *locker) Unlock() {
	// ensures that only the instance with the lock can unlock the distributed mutex
	if !l.hasLock {
		return
	}

	l.requestUnlock <- l

	for {
		select {
		case <-l.ctx.Done():
			return
		case backoff := <-l.unlocked:
			l.mu.Lock()
			l.hasLock = false
			l.mu.Unlock()

			// unlock successful, backoff to allow fair chance for the waiting lockers
			time.Sleep(backoff)
			return
		case <-l.unlockFailed:
			l.requestUnlock <- l
		}
	}
}

func (l *locker) getLeaseID() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leaseID
}

var _ sync.Locker = (*locker)(nil)
