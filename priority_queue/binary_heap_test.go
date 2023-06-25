package priorityqueue

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Test struct {
	id       string
	groupID  string
	priority int64
}

func NewTest(priority int64, groupID string, id string) Test {
	return Test{
		priority: priority,
		groupID:  groupID,
		id:       id,
	}
}

func (t Test) ID() string {
	return t.id
}

func (t Test) GroupID() string {
	return t.groupID
}

func (t Test) Priority() int64 {
	return t.priority
}

func TestBinHeap_Init(t *testing.T) {
	a := []Item{
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(23, uuid.NewString(), uuid.NewString()),
		NewTest(33, uuid.NewString(), uuid.NewString()),
		NewTest(44, uuid.NewString(), uuid.NewString()),
		NewTest(1, uuid.NewString(), uuid.NewString()),
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(4, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(99, uuid.NewString(), uuid.NewString()),
	}

	bh := NewBinHeap[Item](12)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	expected := []int64{
		1,
		2,
		2,
		2,
		2,
		4,
		6,
		23,
		33,
		44,
		99,
	}

	res := make([]int64, 0, 12)

	for i := 0; i < 11; i++ {
		item := bh.ExtractMin()
		item.Priority()
		res = append(res, item.Priority())
	}

	require.Equal(t, expected, res)
}

func TestBinHeap_MaxLen(t *testing.T) {
	a := []Item{
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(23, uuid.NewString(), uuid.NewString()),
		NewTest(33, uuid.NewString(), uuid.NewString()),
		NewTest(44, uuid.NewString(), uuid.NewString()),
		NewTest(1, uuid.NewString(), uuid.NewString()),
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(2, uuid.NewString(), uuid.NewString()),
		NewTest(4, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(99, uuid.NewString(), uuid.NewString()),
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
				pq.Insert(NewTest(rand.Int63(), uuid.NewString(), uuid.NewString())) //nolint:gosec
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
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(23, uuid.NewString(), uuid.NewString()),
		NewTest(33, uuid.NewString(), uuid.NewString()),
		NewTest(44, uuid.NewString(), uuid.NewString()),
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(7, uuid.NewString(), uuid.NewString()),
		NewTest(8, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(99, uuid.NewString(), uuid.NewString()),
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
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(23, uuid.NewString(), uuid.NewString()),
		NewTest(33, uuid.NewString(), uuid.NewString()),
		NewTest(44, uuid.NewString(), uuid.NewString()),
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(7, uuid.NewString(), uuid.NewString()),
		NewTest(8, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(99, uuid.NewString(), uuid.NewString()),
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
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(23, uuid.NewString(), uuid.NewString()),
		NewTest(33, uuid.NewString(), uuid.NewString()),
		NewTest(44, uuid.NewString(), uuid.NewString()),
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(5, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(7, uuid.NewString(), uuid.NewString()),
		NewTest(8, uuid.NewString(), uuid.NewString()),
		NewTest(6, uuid.NewString(), uuid.NewString()),
		NewTest(99, uuid.NewString(), uuid.NewString()),
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

func TestBinHeap_Remove(t *testing.T) {
	a := []Item{
		NewTest(2, "1", "101"),
		NewTest(5, "1", "102"),
		NewTest(99, "1", "103"),
		NewTest(4, "6", "104"),
		NewTest(6, "7", "105"),
		NewTest(23, "2", "106"),
		NewTest(2, "1", "107"),
		NewTest(2, "1", "108"),
		NewTest(33, "3", "109"),
		NewTest(44, "4", "110"),
		NewTest(2, "1", "111"),
	}

	bh := NewBinHeap[Item](12)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	expected := []Item{
		NewTest(4, "6", "104"),
		NewTest(6, "7", "105"),
		NewTest(23, "2", "106"),
		NewTest(33, "3", "109"),
		NewTest(44, "4", "110"),
	}

	out := bh.Remove("1")
	if len(out) != 6 {
		t.Fatal("should be 5")
	}

	for i := 0; i < len(out); i++ {
		if out[i].GroupID() != "1" {
			t.Fatal("id is not 1")
		}
	}

	res := make([]Item, 0, 12)

	for i := 0; i < 5; i++ {
		item := bh.ExtractMin()
		res = append(res, item)
	}

	require.Equal(t, expected, res)
}

func TestExists(t *testing.T) {
	const id = "11111111111"
	a := []Item{
		NewTest(2, "1", id),
		NewTest(5, "1", uuid.NewString()),
		NewTest(99, "1", uuid.NewString()),
		NewTest(4, "6", uuid.NewString()),
		NewTest(6, "7", uuid.NewString()),
		NewTest(23, "2", uuid.NewString()),
		NewTest(2, "1", uuid.NewString()),
		NewTest(2, "1", uuid.NewString()),
		NewTest(33, "3", uuid.NewString()),
		NewTest(44, "4", uuid.NewString()),
		NewTest(2, "1", uuid.NewString()),
	}

	bh := NewBinHeap[Item](12)

	for i := 0; i < len(a); i++ {
		bh.Insert(a[i])
	}

	assert.False(t, bh.Exists("1"))
	assert.True(t, bh.Exists(id))

	_ = bh.Remove("1")

	assert.False(t, bh.Exists(id))
}

func BenchmarkGeneral(b *testing.B) {
	bh := NewBinHeap[Item](100)
	id := uuid.NewString()
	id2 := uuid.NewString()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bh.Insert(NewTest(2, id, id2))
		bh.Remove(id)
	}
}
