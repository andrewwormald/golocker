package golocker_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/andrewwormald/golocker"
	"github.com/corverroos/goku/client/logical"
	"github.com/corverroos/goku/db"
)

// TestLocker is a primitive test and needs to be run with -race flag
func TestLocker(t *testing.T) {
	ctx := context.Background()
	dbc := db.ConnectForTesting(t)
	cl := logical.New(dbc, dbc)

	locker := golocker.New(ctx, "mylocker", dbc, cl)
	go locker.SyncForever()

	var testVariable string
	var wg sync.WaitGroup
	wg.Add(2000)

	mu := locker.NewMutex("testmutex", time.Minute * 2)
	for i := 0; i < 2000; i++ {
		go func(iteration string) {
			mu.Lock()
			testVariable = iteration
			fmt.Println(testVariable)
			mu.Unlock()
			wg.Done()
		}(strconv.FormatInt(int64(i), 10))
	}

	wg.Wait()
}
