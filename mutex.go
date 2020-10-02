package golocker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Mutex struct {
	ctx     context.Context
	uid     string
	leaseID int64
	state   MutexState

	lockAcquired      chan int64 // lease id returned once lock acquired
	lockAcquireFailed chan struct{}
	lockFreed         chan struct{}

	backoff      time.Duration
	lockDuration time.Duration

	requestLock    chan<- string // parent locker's lock request channel
	requestUnlock chan<- int64  // parent locker's release request channel
}

func (m *Mutex) Lock() {
	// pass request to locker
	m.requestLock <- m.uid

	// wait for feedback from locker
	for {
		select {
		case <-m.ctx.Done():
			// context finished, exit to free go routine
			return
		case <-m.lockAcquireFailed:
			// sleep and send new request to locker
			fmt.Println("failed")
			m.requestLock <- m.uid
		case leaseID := <-m.lockAcquired:
			// lock acquired, end blocking pattern
			fmt.Println("locked")
			m.state = MutexLocked
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
			m.state = MutexUnlocked
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
