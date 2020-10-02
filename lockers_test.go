package golocker_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/corverroos/goku/client/logical"
	"github.com/corverroos/goku/db"
	"github.com/luno/jettison/log"

	"github.com/andrewwormald/golocker"
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

func TestRun(t *testing.T) {
	ctx := context.Background()

	dbc, err := sql.Open("mysql", "golocker:@tcp(127.0.0.1:3306)/golocker?parseTime=true")
	if err != nil {
		panic(err)
	}

	err = dbc.Ping()
	if err != nil {
		panic(err)
	}

	dbc.SetMaxOpenConns(10)
	dbc.SetMaxIdleConns(5)
	dbc.SetConnMaxLifetime(time.Second * 10)

	cl := logical.New(dbc, dbc)
	instanceLocker := golocker.New(ctx, "leader_election", dbc, cl)
	db.FillGaps(dbc)
	go instanceLocker.SyncForever()

	m := instanceLocker.NewMutex("isLeader", time.Second * 6)
	for {
		log.Info(ctx, "               ")
		log.Info(ctx, "_______________")
		log.Info(ctx, "requesting lock")
		m.Lock()
		log.Info(ctx, "lock acquired")
		time.Sleep(time.Second * 5)
		m.Unlock()
		log.Info(ctx, "released lock")
	}
}
