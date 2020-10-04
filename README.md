# golocker

## Client

##### `New` returns a new golocker Client type that can create a locker (distributed mutex) and synchronise with other lockers on different goroutines or binaries. Only one Client per binary is needed as it can create multiple distributed mutexes that synchronise their locking with one another.

##### The `Client` can be passed around safely to create mutexes where needed. See NewLocker for more information on using the golocker's Locker (distributed mutex).
```golang
func New(ctx context.Context, globalName string, dbc *sql.DB, gcl goku.Client) *Client
``` 

##### `NewLocker` returns a sync.Locker complaint distributed mutex. In order for lockers to sync their locking, provide the same globalName to the NewMutex method. If a different name is given, then it is a different distributed mutex.
##### Do not copy lockers. Instead, create a new locker with the same globalName.
```golang
func (c *Client) NewLocker(globalName string, autoExpireLockAfter time.Duration) sync.Locker 
```

##### `SyncForever` must be called in order for the lockers to Lock and Unlock.

##### `SyncForever` handles the filling or reflex event gaps, forever expiring goku leases, and synchronising the lockers' locks and unlocks.
```golang
func (c *Client) SyncForever()
```

## Locker
#### Locker fulfills the `sync.Locker` interface. If two or more Lockers are created with the same global name and have access to the same database (even if they are produced from different Clients) they will attempt to obtain the same lock. Therefore they will be able to block one another (by one of them obtaining the lock) even on different hosts. 
