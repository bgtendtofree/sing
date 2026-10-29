package cache

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentStoreAndLoadSnapshot(t *testing.T) {
	c := New[string, int64]()
	c.StoreWithExpire("key", 1, time.Unix(1, 0))
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range 10000 {
				value := int64(worker*10000 + i + 1)
				c.StoreWithExpire("key", value, time.Unix(value, 0))
			}
		}()
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 10000 {
				value, expiry, loaded := c.LoadWithExpire("key")
				if !loaded || value != expiry.Unix() {
					t.Errorf("inconsistent snapshot: value=%d expiry=%v loaded=%v", value, expiry, loaded)
					return
				}
				if _, loaded := c.Load("key"); !loaded {
					t.Error("stored key not found")
					return
				}
			}
		}()
	}
	workers.Wait()
}

func TestLoadSnapshotExpiryAndMiss(t *testing.T) {
	c := New[string, int](WithAge[string, int](60), WithUpdateAgeOnGet[string, int]())
	if value, loaded := c.Load("missing"); loaded || value != 0 {
		t.Fatalf("Load miss = (%d, %v)", value, loaded)
	}
	if value, expiry, loaded := c.LoadWithExpire("missing"); loaded || value != 0 || !expiry.IsZero() {
		t.Fatalf("LoadWithExpire miss = (%d, %v, %v)", value, expiry, loaded)
	}
	c.StoreWithExpire("expired", 1, time.Now().Add(-time.Minute))
	if _, loaded := c.Load("expired"); loaded {
		t.Fatal("expired value returned")
	}
	c.StoreWithExpire("key", 2, time.Now().Add(10*time.Second))
	before := time.Now().Unix()
	value, expiry, loaded := c.LoadWithExpire("key")
	if !loaded || value != 2 || expiry.Unix() < before+60 || expiry.Unix() > time.Now().Unix()+60 {
		t.Fatalf("age refresh snapshot = (%d, %v, %v)", value, expiry, loaded)
	}
}
