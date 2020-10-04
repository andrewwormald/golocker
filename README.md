# golocker

### Locker
#### What is a Locker
A Locker is the single entry way to creating M   
#### The Locker API
##### New returns a new golocker Locker type that can create a distributed mutex and synchronise with other mutexes on different goroutines or binaries. Only one locker per binary is needed as it can create multiple distributed mutexes that synchronise their locking with one another.

##### The Locker can be passed around safely to create mutexes where needed. See NewMutex for more information on using the golocker Mutex.   
```golang
func New(ctx context.Context, globalName string, dbc *sql.DB, gcl goku.Client) *Locker 
```

##### NewMutex returns a sync.Locker complaint distributed mutex. In order for mutexes to sync locking, provide the same globalName to the NewMutex method.

##### **Do not copy mutexes**. Instead, create a new mutex with the same globalName.
```golang
func (l *Locker) NewMutex(globalName string, autoExpireLockAfter time.Duration) sync.Locker 
````

##### SyncForever must be called in order for the mutexes to Lock and Unlock. SyncForever handles the filling or reflex event gaps, forever expiring goku leases, and synchronising the mutexes locks and unlocks.
```
func (l *Locker) SyncForever() 
```

Mutex API
```golang
Lock()

Unlock()
```
