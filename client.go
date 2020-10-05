package golocker

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pborman/uuid"
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

type Client struct {
	ctx  context.Context
	dbc  *sql.DB
	goku goku.Client

	retryBackoff time.Duration
	errorBackoff time.Duration

	mu   sync.Mutex
	pool map[string]*locker

	lockRequests   chan *locker
	unlockRequests chan *locker
}

// New returns a new golocker Client type that can create a locker (distributed mutex) and synchronise with other lockers
// on different goroutines or binaries. Only one Client per binary is needed as it can create multiple distributed mutexes
// that synchronise their locking with one another.
//
// The Client can be passed around safely to create mutexes where needed. See NewLocker for more information on using
// the golocker's Locker (distributed mutex).
func New(ctx context.Context, dbc *sql.DB, gcl goku.Client, opts ...Option) *Client {
	cl := &Client{
		ctx:            ctx,
		retryBackoff:   time.Microsecond * 100,
		errorBackoff:   time.Second * 10,
		dbc:            dbc,
		goku:           gcl,
		pool:           make(map[string]*locker),
		lockRequests:   make(chan *locker),
		unlockRequests: make(chan *locker),
	}

	for _, opt := range opts {
		opt(cl)
	}

	return cl
}

type Option func(cl *Client)

// WithRetryBackoff configures the interval between when a Locker fails to obtain the lock and when it tries again.
func WithRetryBackoff(retryBackoff time.Duration) Option {
	return func(cl *Client) {
		cl.retryBackoff = retryBackoff
	}
}

// WithErrorBackoff configures the time it takes for the Locker to retry obtaining the lock after an unexpected error occurs.
func WithErrorBackoff(errorBackoff time.Duration) Option {
	return func(cl *Client) {
		cl.errorBackoff = errorBackoff
	}
}

// NewLocker returns a sync.Locker complaint distributed mutex. In order for lockers to sync their locking, provide the same
// globalName to the NewMutex method. If a different name is given, then it is a different distributed mutex.
//
// Do not copy lockers. Instead, create a new locker with the same globalName.
func (c *Client) NewLocker(globalName string, autoExpireLockAfter time.Duration) sync.Locker {
	lckr := newLocker(c.ctx, globalName, autoExpireLockAfter, c.lockRequests, c.unlockRequests)

	c.mu.Lock()
	c.pool[lckr.instanceIdentifier] = lckr
	c.mu.Unlock()

	return lckr
}

// SyncForever enables the lockers to work as distributed mutexes. SyncForever is a blocking call.
//
// SyncForever handles the filling or reflex event gaps, forever expiring goku leases, and synchronising the lockers'
// locks and unlocks.
func (c *Client) SyncForever() {
	db.FillGaps(c.dbc)
	go c.processLockRequestsForever()
	go c.processUnlockRequestsForever()
	go db.ExpireLeasesForever(c.dbc)

	c.manageMutexesForever()
}

func (c *Client) processUnlockRequestsForever() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case lckr := <-c.unlockRequests:
			err := c.goku.ExpireLease(c.ctx, lckr.getLeaseID())
			if errors.Is(err, goku.ErrUpdateRace) {
				// more of a sanity check as this shouldn't ever happen
				fmt.Print("update race, retrying")
				c.unlockRequests <- lckr
				continue
			} else if errors.Is(err, goku.ErrLeaseNotFound) {
				// can consider successful release
			} else if err != nil {
				// log error, backoff, and retry
				log.Error(c.ctx, err)
				time.Sleep(c.errorBackoff)

				// one locker will have the lock and thus safe to pass the locker back into the channel without
				// it becoming blocked
				c.unlockRequests <- lckr
				continue
			}

			lckr.unlocked <- c.retryBackoff
		default:
			continue
		}
	}
}

func (c *Client) processLockRequestsForever() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case lckr := <-c.lockRequests:
			err := c.setLock(lckr)
			if errors.IsAny(err, goku.ErrConditional, goku.ErrUpdateRace, errLeaseHasNotExpired) {
				// default pattern, continue to try acquire the lock until successful
				lckr.lockingFailed <- c.retryBackoff
			} else if err != nil {
				// log error and notify locker of failed attempt
				log.Error(c.ctx, err)
				lckr.lockingFailed <- c.errorBackoff
			}
		default:
			continue
		}
	}
}

func (c *Client) manageMutexesForever() {
	streamFunc := c.goku.Stream("golocker/locks/")
	consumer := reflex.NewConsumer("golocker" + uuid.New(), c.consumerFunc())
	spec := reflex.NewSpec(streamFunc, rpatterns.MemCursorStore(), consumer, reflex.WithStreamFromHead())
	rpatterns.RunForever(
		func() context.Context {
			return c.ctx
		},
		spec,
	)
}

func (c *Client) consumerFunc() func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
	return func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		switch goku.EventType(event.Type.ReflexType()) {
		case goku.EventTypeSet:
			mutex, exists := c.pool[string(event.MetaData)]
			if !exists {
				// Must exist in another instance either in the same binary or another host
				return nil
			}

			kv, err := c.goku.Get(c.ctx, event.ForeignID)
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

var errLeaseHasNotExpired = errors.New("lease has not expired yet")

func (c *Client) setLock(mu *locker) error {
	key := c.keyForMutex(mu)
	kv, err := c.goku.Get(c.ctx, key)
	if errors.IsAny(err, goku.ErrNotFound) {
		// continue
	} else if err != nil {
		return err
	}

	if len(kv.Value) != 0 {
		return errLeaseHasNotExpired
	}

	return c.goku.Set(
		c.ctx,
		key,
		[]byte(mu.instanceIdentifier),
		goku.WithPrevVersion(kv.Version),
		goku.WithExpiresAt(time.Now().Add(mu.autoExpireLockAfter)))
}

func (c *Client) keyForMutex(mu *locker) string {
	return "golocker/locks/" + mu.distributedIdentifier
}
