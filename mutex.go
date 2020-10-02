package golocker

import (
	"context"
	"sync"
	"time"
)

type Mutex struct {
	ctx                   context.Context
	distributedIdentifier string // used to sync mutexes across goroutines and hosts
	leaseID               int64

	mu    sync.Mutex
	state MutexState // potentially remove as it seems useless and potential data race if not looked after

	lockAcquired      chan int64 // lease id returned once lock acquired
	lockAcquireFailed chan struct{}
	lockFreed         chan struct{}

	autoExpireLockAfter time.Duration

	requestLock   chan<- string // parent locker's lock request channel
	requestUnlock chan<- int64  // parent locker's release request channel
}

func (m *Mutex) Lock() {
	// pass request to locker
	m.requestLock <- m.distributedIdentifier

	// wait for feedback from locker
	for {
		select {
		case <-m.ctx.Done():
			// context finished, exit to free go routine
			return
		case <-m.lockAcquireFailed:
			// sleep and send new request to locker
			m.requestLock <- m.distributedIdentifier
		case leaseID := <-m.lockAcquired:
			// lock acquired, end blocking pattern
			m.mu.Lock()
			m.state = MutexLocked
			m.mu.Unlock()
			m.leaseID = leaseID
			return
		default:
			continue
		}
	}
}

func (m *Mutex) Unlock() {
	// request locker frees lock for this mutex
	m.requestUnlock <- m.leaseID
	for {
		select {
		case <-m.ctx.Done():
			// context finished, exit to free go routine
			return
		case <-m.lockFreed:
			// lock freed, end blocking pattern
			m.mu.Lock()
			m.state = MutexUnlocked
			m.mu.Unlock()
			return
		default:
			continue
		}
	}
}

type MutexState int32

const (
	MutexUnlocked MutexState = 0 // default
	MutexLocked   MutexState = 1
)

var _ sync.Locker = (*Mutex)(nil)
