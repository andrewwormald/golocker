package golocker

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/corverroos/goku"
	"github.com/corverroos/goku/db"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/pborman/uuid"
)

type Locker struct {
	ctx  context.Context
	name string

	dbc  *sql.DB
	goku goku.Client

	mu   sync.Mutex
	pool map[string]*Mutex

	lockRequests        chan string // using the mutex's uid
	releaseLockRequests chan int64  // using the mutex's lease id
}

func New(ctx context.Context, globalName string, dbc *sql.DB, gcl goku.Client) *Locker {
	return &Locker{
		ctx:                 ctx,
		name:                globalName,
		dbc:                 dbc,
		goku:                gcl,
		pool:                make(map[string]*Mutex),
		lockRequests:        make(chan string),
		releaseLockRequests: make(chan int64),
	}
}

func (l *Locker) NewMutex(backoff, maxLockDuration time.Duration) *Mutex {
	mutex := &Mutex{
		ctx:               l.ctx,
		uid:               uuid.New(),
		lockAcquired:      make(chan int64),
		lockAcquireFailed: make(chan struct{}),
		lockFreed:         make(chan struct{}),
		backoff:           backoff,
		lockDuration:      maxLockDuration,
		requestLock:       l.lockRequests,
		requestUnlock:     l.releaseLockRequests,
	}

	// NOTE: Small chance of uuid generating a key twice
	l.mu.Lock()
	l.pool[mutex.uid] = mutex
	l.mu.Unlock()

	return mutex
}

func (l *Locker) SyncForever() {
	go l.processLockRequestsForever()
	go l.processReleaseLockRequestsForever()

	l.manageMutexesForever()
}

func (l *Locker) processReleaseLockRequestsForever() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case leaseID := <-l.releaseLockRequests:
			err := l.goku.ExpireLease(l.ctx, leaseID)
			if err != nil {
				// log error and retry the request
				log.Error(l.ctx, err)
				l.releaseLockRequests <- leaseID
				continue
			}
		default:
			continue
		}
	}
}

func (l *Locker) processLockRequestsForever() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case mutexUID := <-l.lockRequests:
			mu, exists := l.pool[mutexUID]
			if !exists {
				continue
			}

			err := l.setLock(mu)
			if errors.IsAny(err, goku.ErrConditional, goku.ErrUpdateRace, ErrLeaseHasNotExpired) {
				// retryQ
				mu.lockAcquireFailed <- struct{}{}
			} else if err != nil {
				// log error and notify mutex of failed attempt
				log.Error(l.ctx, err)
				mu.lockAcquireFailed <- struct{}{}
			}
		default:
			continue
		}
	}
}

func (l *Locker) manageMutexesForever() {
	fn := l.goku.Stream("golocker/locks/" + l.name)
	c := reflex.NewConsumer(l.name, l.consumerFunc())
	spec := reflex.NewSpec(fn, rpatterns.MemCursorStore(), c)
	rpatterns.RunForever(
		func() context.Context {
			return l.ctx
		},
		spec,
	)
}

func (l *Locker) consumerFunc() func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
	return func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		mutex, exists := l.pool[event.ForeignID]
		if !exists {
			// Must exist in another instance either in the same binary or another host
			return nil
		}

		switch event.Type {
		case goku.EventTypeSet:
			kv, err := l.goku.Get(l.ctx, l.keyForMutex(mutex))
			if err != nil {
				return err
			}

			mutex.lockAcquired <- kv.LeaseID
			return nil
		case goku.EventTypeDelete:
			mutex.lockFreed <- struct{}{}
			return nil
		case goku.EventTypeExpire:
			mutex.lockFreed <- struct{}{}
			return nil
		default:
			// skip unknown events
			return nil
		}
	}
}

func (l *Locker) setLock(mu *Mutex) error {
	key := l.keyForMutex(mu)
	kv, err := l.goku.Get(l.ctx, key)
	if errors.Is(err, goku.ErrNotFound) {
		// continue
	} else if err != nil {
		return err
	}

	//kv.LeaseID
	leases, err := db.ListLeasesToExpire(l.ctx, l.dbc, time.Now())
	if err != nil {
		return err
	}

	var hasExpired bool
	for _, lease := range leases {
		if lease.ID == kv.LeaseID {
			hasExpired = true
		}
	}

	if !hasExpired && kv.LeaseID != 0 {
		return ErrLeaseHasNotExpired
	}

	return l.goku.Set(
		l.ctx,
		key,
		[]byte(nil),
		goku.WithPrevVersion(kv.Version),
		goku.WithExpiresAt(time.Now().Add(mu.lockDuration)))
}

func (l *Locker) keyForMutex(mu *Mutex) string {
	return "golocker/locks/" + l.name + "/" + mu.uid
}
