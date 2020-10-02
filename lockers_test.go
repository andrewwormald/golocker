package golocker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/andrewwormald/golocker"
	"github.com/corverroos/goku/client/logical"
	"github.com/corverroos/goku/db"
)

func TestLocker(t *testing.T) {
	ctx := context.Background()
	dbc := db.ConnectForTesting(t)
	cl := logical.New(dbc, dbc)

	locker := golocker.New(ctx, "mylocker", dbc, cl)
	go locker.SyncForever()
	go db.ExpireLeasesForever(dbc)

	var testVariable string
	var wg sync.WaitGroup
	wg.Add(2)

	mu := locker.NewMutex(time.Second, time.Second * 2)
	//go func() {
	//	mu.Lock()
	//	defer mu.Unlock()
	//	testVariable = "no data"
	//	wg.Done()
	//}()

	go func() {
		mu.Lock()
		testVariable = "race detected"
		mu.Unlock()
		wg.Done()
	}()

	wg.Wait()
	fmt.Println(testVariable)
}
