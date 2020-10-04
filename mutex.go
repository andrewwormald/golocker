package golocker

import (
	"context"
	"github.com/pborman/uuid"
	"sync"
	"time"
)

type Mutex struct {
	ctx                   context.Context
	distributedIdentifier string // used to sync mutexes across goroutines and hosts
	instanceIdentifier    string // used for identifying and discriminating between mutex instances
	leaseID               int64

	mu    sync.Mutex
	state MutexState // potentially remove as it seems useless and potential data race if not looked after

	locked          chan int64 // lease id returned once lock acquired
	lockingFailed   chan struct{}
	unlocked        chan struct{}
	unlockingFailed chan struct{}

	autoExpireLockAfter time.Duration

	requestLock   chan<- *Mutex // parent locker's lock request channel
	requestUnlock chan<- *Mutex // parent locker's release request channel
}

func newMutex(ctx context.Context, distributedIdentifier string, autoExpireLockAfter time.Duration,
	lockRequests chan *Mutex, unlockRequests chan *Mutex) *Mutex {
	return &Mutex{
		ctx:                   ctx,
		distributedIdentifier: distributedIdentifier,
		instanceIdentifier:    uuid.New(),
		locked:                make(chan int64),
		lockingFailed:         make(chan struct{}),
		unlocked:              make(chan struct{}),
		unlockingFailed:       make(chan struct{}),
		autoExpireLockAfter:   autoExpireLockAfter,
		requestLock:           lockRequests,
		requestUnlock:         unlockRequests,
	}
}

func (m *Mutex) Lock() {
	// pass request to locker
	m.requestLock <- m

	// wait for feedback from locker
	for {
		select {
		case <-m.ctx.Done():
			// context finished, exit to free go routine
			return
		case <-m.lockingFailed:
			// send new request to locker
			m.requestLock <- m
		case leaseID := <-m.locked:
			// lock acquired, end blocking pattern
			m.mu.Lock()
			m.state = MutexLocked
			m.leaseID = leaseID
			m.mu.Unlock()
			return
		default:
			continue
		}
	}
}

func (m *Mutex) Unlock() {
	m.requestUnlock <- m
}

func (m *Mutex) getLeaseID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leaseID
}

func (m *Mutex) resetLeaseID() {
	m.mu.Lock()
	m.leaseID = 0
	m.mu.Unlock()
}

type MutexState int32

const (
	MutexUnlocked MutexState = 0 // default
	MutexLocked   MutexState = 1
)

var _ sync.Locker = (*Mutex)(nil)
