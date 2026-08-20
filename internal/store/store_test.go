package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	t.Parallel()
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
	if s.Len() != 0 {
		t.Errorf("expected length 0, got %d", s.Len())
	}
}

func TestSetAndGet(t *testing.T) {
	t.Parallel()
	s := New()
	s.Set("key1", "value1")
	
	val, ok := s.Get("key1")
	if !ok {
		t.Error("expected key1 to be found")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %s", val)
	}
}

func TestGetMissing(t *testing.T) {
	t.Parallel()
	s := New()
	
	val, ok := s.Get("missing")
	if ok {
		t.Error("expected missing key to not be found")
	}
	if val != "" {
		t.Errorf("expected empty string, got %s", val)
	}
}

func TestSetOverwrite(t *testing.T) {
	t.Parallel()
	s := New()
	s.Set("key1", "value1")
	s.Set("key1", "value2")
	
	val, ok := s.Get("key1")
	if !ok {
		t.Error("expected key1 to be found")
	}
	if val != "value2" {
		t.Errorf("expected value2, got %s", val)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	s := New()
	s.Set("key1", "value1")
	
	ok := s.Delete("key1")
	if !ok {
		t.Error("expected Delete to return true for existing key")
	}
	
	_, ok = s.Get("key1")
	if ok {
		t.Error("expected key1 to be deleted")
	}
}

func TestDeleteMissing(t *testing.T) {
	t.Parallel()
	s := New()
	
	ok := s.Delete("missing")
	if ok {
		t.Error("expected Delete to return false for missing key")
	}
}

func TestLen(t *testing.T) {
	t.Parallel()
	s := New()
	if s.Len() != 0 {
		t.Errorf("expected length 0, got %d", s.Len())
	}
	
	s.Set("key1", "val1")
	s.Set("key2", "val2")
	
	if s.Len() != 2 {
		t.Errorf("expected length 2, got %d", s.Len())
	}
	
	s.Delete("key1")
	if s.Len() != 1 {
		t.Errorf("expected length 1, got %d", s.Len())
	}
}

func TestKeys(t *testing.T) {
	t.Parallel()
	s := New()
	s.Set("key1", "val1")
	s.Set("key2", "val2")
	
	keys := s.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	
	hasKey1 := false
	hasKey2 := false
	for _, k := range keys {
		if k == "key1" {
			hasKey1 = true
		}
		if k == "key2" {
			hasKey2 = true
		}
	}
	
	if !hasKey1 || !hasKey2 {
		t.Errorf("expected keys to contain key1 and key2, got %v", keys)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	s := New()
	var wg sync.WaitGroup
	
	const numGoroutines = 100
	const numOps = 100
	
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key-%d", j%10)
				val := fmt.Sprintf("val-%d-%d", id, j)
				
				// Mix of operations
				if j%3 == 0 {
					s.Set(key, val)
				} else if j%3 == 1 {
					s.Get(key)
				} else {
					s.Delete(key)
				}
				s.Len()
				if j%10 == 0 {
					s.Keys()
				}
			}
		}(i)
	}
	
	wg.Wait()
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	t.Parallel()
	s := New()
	s.Set("key1", "val1")
	
	var wg sync.WaitGroup
	startCh := make(chan struct{})
	
	// Launch readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			for j := 0; j < 100; j++ {
				s.Get("key1")
				s.Len()
				s.Keys()
			}
		}()
	}
	
	// Launch writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-startCh
		for j := 0; j < 100; j++ {
			s.Set(fmt.Sprintf("key-%d", j), "val")
		}
	}()
	
	close(startCh) // Start all goroutines at once
	wg.Wait()
}
