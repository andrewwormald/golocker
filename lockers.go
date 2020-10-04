package golocker

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/corverroos/goku"
	"github.com/corverroos/goku/db"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

type Locker struct {
	ctx  context.Context
	name string

	dbc  *sql.DB
	goku goku.Client

	mu   sync.Mutex
	pool map[string]*Mutex

	lockRequests   chan *Mutex
	unlockRequests chan *Mutex
}

func New(ctx context.Context, globalName string, dbc *sql.DB, gcl goku.Client) *Locker {
	db.FillGaps(dbc)

	return &Locker{
		ctx:            ctx,
		name:           globalName,
		dbc:            dbc,
		goku:           gcl,
		pool:           make(map[string]*Mutex),
		lockRequests:   make(chan *Mutex),
		unlockRequests: make(chan *Mutex),
	}
}

func (l *Locker) NewMutex(distributedIdentifier string, autoExpireLockAfter time.Duration) *Mutex {
	mu := newMutex(l.ctx, distributedIdentifier, autoExpireLockAfter, l.lockRequests, l.unlockRequests)

	l.mu.Lock()
	l.pool[mu.instanceIdentifier] =  mu
	l.mu.Unlock()

	return mu
}

func (l *Locker) SyncForever() {
	go l.processLockRequestsForever()
	go l.processUnlockRequestsForever()
	go db.ExpireLeasesForever(l.dbc)

	l.manageMutexesForever()
}

func (l *Locker) processUnlockRequestsForever() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case mutex := <-l.unlockRequests:
			err := l.goku.ExpireLease(l.ctx, mutex.getLeaseID())
			if errors.Is(err, goku.ErrUpdateRace) {
				// more of a sanity check as this shouldn't happen unless 2 instances have the
				// lock which should never happen.
				l.unlockRequests <- mutex
				continue
			} else if errors.Is(err, goku.ErrLeaseNotFound) {
				// in case of bad data so just move on. No need to do anything.
				continue
			} else if err != nil {
				// log error and allow for auto expire to release lock.
				log.Error(l.ctx, err)
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
		case mutex := <-l.lockRequests:
			err := l.setLock(mutex)
			if errors.IsAny(err, goku.ErrConditional, goku.ErrUpdateRace, ErrLeaseHasNotExpired) {
				// default pattern, continue to try acquire the lock until successful
				time.Sleep(time.Second)
				fmt.Println("retry acquire lock")
				mutex.lockingFailed <- struct{}{}
			} else if errors.IsAny(err, goku.ErrLeaseNotFound) {
				// sanity check in case of bad data
				mutex.resetLeaseID()
				mutex.lockingFailed <- struct{}{}
			} else if err != nil {
				// log error and notify mutex of failed attempt
				log.Error(l.ctx, err)
				time.Sleep(time.Second)
				mutex.lockingFailed <- struct{}{}
			}
		default:
			continue
		}
	}
}

func (l *Locker) manageMutexesForever() {
	fn := l.goku.Stream("golocker/locks/" + l.name)
	c := reflex.NewConsumer(l.name, l.consumerFunc())
	spec := reflex.NewSpec(fn, rpatterns.MemCursorStore(), c, reflex.WithStreamFromHead())
	rpatterns.RunForever(
		func() context.Context {
			return l.ctx
		},
		spec,
	)
}

func (l *Locker) consumerFunc() func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
	return func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		switch goku.EventType(event.Type.ReflexType()) {
		case goku.EventTypeSet:
			mutex, exists := l.pool[string(event.MetaData)]
			if !exists {
				// Must exist in another instance either in the same binary or another host
				return nil
			}

			kv, err := l.goku.Get(l.ctx, event.ForeignID)
			if errors.Is(err, goku.ErrNotFound) {
				// continue
			} else if err != nil {
				return err
			}

			// sanity check
			if string(kv.Value) == mutex.instanceIdentifier {
				mutex.locked <- kv.LeaseID
			}
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
	if errors.IsAny(err, goku.ErrNotFound) {
		// continue
	} else if err != nil {
		return err
	}

	if len(kv.Value) != 0 {
		return ErrLeaseHasNotExpired
	}

	return l.goku.Set(
		l.ctx,
		key,
		[]byte(mu.instanceIdentifier),
		goku.WithPrevVersion(kv.Version),
		goku.WithExpiresAt(time.Now().Add(mu.autoExpireLockAfter)))
}

func (l *Locker) keyForMutex(mu *Mutex) string {
	return "golocker/locks/" + l.name + "/" + mu.distributedIdentifier
}
