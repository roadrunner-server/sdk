package priorityqueue

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Test struct {
	priority int64
}

func NewTest(priority int64) Test {
	return Test{
		priority: priority,
	}
}

func (t Test) Body() []byte {
	return nil
}

func (t Test) Context() ([]byte, error) {
	return nil, nil
}

func (t Test) ID() string {
	return "none"
}

func (t Test) Priority() int64 {
	return t.priority
}

func TestBinHeap_Init(t *testing.T) {
	a := []Item{
		NewTest(2),
		NewTest(23),
		NewTest(33),
		NewTest(44),
		NewTest(1),
		NewTest(2),
		NewTest(2),
		NewTest(2),
		NewTest(4),
		NewTest(6),
		NewTest(99),
	}

	bh := NewBinHeap[Item](12)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	expected := []Item{
		NewTest(1),
		NewTest(2),
		NewTest(2),
		NewTest(2),
		NewTest(2),
		NewTest(4),
		NewTest(6),
		NewTest(23),
		NewTest(33),
		NewTest(44),
		NewTest(99),
	}

	res := make([]Item, 0, 12)

	for i := 0; i < 11; i++ {
		item := bh.ExtractMin()
		res = append(res, item)
	}

	require.Equal(t, expected, res)
}

func TestBinHeap_MaxLen(t *testing.T) {
	a := []Item{
		NewTest(2),
		NewTest(23),
		NewTest(33),
		NewTest(44),
		NewTest(1),
		NewTest(2),
		NewTest(2),
		NewTest(2),
		NewTest(4),
		NewTest(6),
		NewTest(99),
	}

	bh := NewBinHeap[Item](1)

	go func() {
		res := make([]Item, 0, 12)

		for i := 0; i < 11; i++ {
			item := bh.ExtractMin()
			res = append(res, item)
		}
		require.Equal(t, 11, len(res))
	}()

	time.Sleep(time.Second)
	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	time.Sleep(time.Second)
}

func TestNewPriorityQueue(t *testing.T) {
	insertsPerSec := uint64(0)
	getPerSec := uint64(0)
	stopCh := make(chan struct{}, 1)
	pq := NewBinHeap[Item](1000)

	go func() {
		tt3 := time.NewTicker(time.Millisecond * 10)
		for {
			select {
			case <-tt3.C:
				require.Less(t, pq.Len(), uint64(1002))
			case <-stopCh:
				return
			}
		}
	}()

	go func() {
		tt := time.NewTicker(time.Second)

		for {
			select {
			case <-tt.C:
				fmt.Printf("Insert per second: %d\n", atomic.LoadUint64(&insertsPerSec))
				atomic.StoreUint64(&insertsPerSec, 0)
				fmt.Printf("ExtractMin per second: %d\n", atomic.LoadUint64(&getPerSec))
				atomic.StoreUint64(&getPerSec, 0)
			case <-stopCh:
				tt.Stop()
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				pq.ExtractMin()
				atomic.AddUint64(&getPerSec, 1)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				pq.Insert(NewTest(rand.Int63())) //nolint:gosec
				atomic.AddUint64(&insertsPerSec, 1)
			}
		}
	}()

	time.Sleep(time.Second * 5)
	stopCh <- struct{}{}
	stopCh <- struct{}{}
	stopCh <- struct{}{}
	stopCh <- struct{}{}
}

func TestNewItemWithTimeout(t *testing.T) {
	a := []Item{
		NewTest(5),
		NewTest(23),
		NewTest(33),
		NewTest(44),
		NewTest(5),
		NewTest(5),
		NewTest(6),
		NewTest(7),
		NewTest(8),
		NewTest(6),
		NewTest(99),
	}

	/*
		first item should be extracted not less than 5 seconds after we call ExtractMin
		5 seconds is a minimum timeout for our items
	*/
	bh := NewBinHeap[Item](100)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	tn := time.Now()
	item := bh.ExtractMin()
	assert.Equal(t, int64(5), item.Priority())
	assert.GreaterOrEqual(t, float64(5), time.Since(tn).Seconds())
}

func TestItemPeek(t *testing.T) {
	a := []Item{
		NewTest(5),
		NewTest(23),
		NewTest(33),
		NewTest(44),
		NewTest(5),
		NewTest(5),
		NewTest(6),
		NewTest(7),
		NewTest(8),
		NewTest(6),
		NewTest(99),
	}

	/*
		first item should be extracted not less than 5 seconds after we call ExtractMin
		5 seconds is a minimum timeout for our items
	*/
	bh := NewBinHeap[Item](100)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	tmp := bh.PeekPriority()
	assert.Equal(t, int64(5), tmp)

	tn := time.Now()
	item := bh.ExtractMin()
	assert.Equal(t, int64(5), item.Priority())
	assert.GreaterOrEqual(t, float64(5), time.Since(tn).Seconds())
}

func TestItemPeekConcurrent(t *testing.T) {
	a := []Item{
		NewTest(5),
		NewTest(23),
		NewTest(33),
		NewTest(44),
		NewTest(5),
		NewTest(5),
		NewTest(6),
		NewTest(7),
		NewTest(8),
		NewTest(6),
		NewTest(99),
	}

	/*
		first item should be extracted not less than 5 seconds after we call ExtractMin
		5 seconds is a minimum timeout for our items
	*/
	bh := NewBinHeap[Item](100)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			tmp := bh.PeekPriority()
			_ = tmp
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 11; i++ {
			min := bh.ExtractMin()
			_ = min
		}
	}()

	wg.Wait()
}
