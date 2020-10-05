package golocker_test

import (
	"context"
	"database/sql"
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
	//t.Skip()

	ctx := context.Background()
	dbc := db.ConnectForTesting(t)
	cl := logical.New(dbc, dbc)

	locker := golocker.New(ctx, dbc, cl)
	go locker.SyncForever()

	var testVariable string
	var wg sync.WaitGroup
	wg.Add(2000)

	mu := locker.NewLocker("is_leader", time.Minute * 2)
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

func TestBin(t *testing.T) {
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
	client := golocker.New(ctx, dbc, cl)
	go client.SyncForever()

	m := client.NewLocker("isLeader", time.Second * 10)

	// this can be run in a for loop to switch between instances forever
	for {
		fmt.Println("waiting")
		m.Lock()
		time.Sleep(time.Second * 5)
		fmt.Println("locked")
		m.Unlock()
		fmt.Println("unlocked")
	}
}
