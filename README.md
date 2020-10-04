# golocker

###### An example of how to use golocker can be found here: https://github.com/andrewwormald/example-golocker

## Client

##### `New` returns a new golocker Client type that can create a locker (distributed mutex) and synchronise with other lockers on different goroutines or binaries. Only one Client per binary is needed as it can create multiple distributed mutexes that synchronise their locking with one another.

##### The `Client` can be passed around safely to create mutexes where needed. See NewLocker for more information on using the golocker's Locker (distributed mutex).
```golang
func New(ctx context.Context, dbc *sql.DB, gcl goku.Client, opts ...Option) *Client 
``` 

##### `NewLocker` returns a sync.Locker complaint distributed mutex. In order for lockers to sync their locking, provide the same globalName to the NewMutex method. If a different name is given, then it is a different distributed mutex.
##### Do not copy lockers. Instead, create a new locker with the same globalName.
```golang
func (c *Client) NewLocker(globalName string, autoExpireLockAfter time.Duration) sync.Locker 
```

##### `SyncForever` enables the lockers to work as distributed mutexes. `SyncForever` is a blocking call.

##### `SyncForever` handles the filling or reflex event gaps, forever expiring goku leases, and synchronising the lockers' locks and unlocks.
```golang
func (c *Client) SyncForever()
```

## Locker
##### Locker fulfills the `sync.Locker` interface. If two or more Lockers are created with the same global name and have access to the same database (even if they are produced from different Clients) they will attempt to obtain the same lock. Therefore they will be able to block one another (by one of them obtaining the lock) even on different hosts. 

##### `Lock` is a blocking call until the lock has been acquired.
```golang
func (l *locker) Lock()
```

##### `Unlock` unlocks the distributed locker
```golang
func (l *locker) Unlock()
```

## Options
```golang
type Option func(cl *Client)
```

##### `WithRetryBackoff` configures the interval between when a Locker fails to obtain the lock and when it tries again.
```golang
func WithRetryBackoff(retryBackoff time.Duration) Option
```

##### `WithErrorBackoff` configures the time it takes for the Locker to retry obtaining the lock after an unexpected error occurs.
```golang
func WithErrorBackoff(errorBackoff time.Duration) Option
```
