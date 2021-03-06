package gopool_test

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jettyu/gopool"
)

type testElem struct {
	gopool.ElemBase
	active int
	pool   gopool.Pool
}

func newTestElem(pool gopool.Pool) (gopool.Elem, error) {
	return &testElem{
		ElemBase: gopool.NewElemBase(),
		pool:     pool,
	}, nil
}

func (p *testElem) Close() error {
	if p.IsAlive() {
		return p.pool.Put(p)
	}
	return nil
}

func (p *testElem) Out() error {
	p.active = -1
	return nil
}

func (p *testElem) IsAlive() bool {
	if p.active < 0 {
		return false
	}
	p.active++
	return true
}

var _ gopool.Elem = (*testElem)(nil)

func TestChanPool(t *testing.T) {
	maxActive := 10
	maxIdle := 3
	pl := gopool.NewChanPool(newTestElem,
		maxActive, maxIdle, time.Millisecond*10, time.Millisecond*10)
	defer pl.Close()
	elems := make(chan *testElem, maxActive)
	var wg sync.WaitGroup
	for i := 0; i < maxActive; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			elem, e := pl.Get()
			if e != nil {
				t.Fatal(e)
			}
			elems <- elem.(*testElem)
		}(i)
	}
	wg.Wait()
	_, e := pl.Get()
	if e != os.ErrNotExist {
		t.Fatal(e)
	}
	for i := 0; i < maxActive; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			elem := <-elems
			if e := elem.Close(); e != nil {
				t.Fatal(e)
			}
		}(i)
	}
	wg.Wait()
	if pl.Len() != maxActive {
		t.Fatal(pl.Len())
	}
	<-time.After(time.Second)
	for i := 0; i < maxActive; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			elem, e := pl.Get()
			if e != nil {
				t.Fatal(e)
			}
			if !elem.IsAlive() {
				t.Fatal(elem)
			}
			elem.(*testElem).Close()
		}(i)
	}
	wg.Wait()
	if pl.Len() != maxIdle {
		t.Fatal(pl.Len(), maxIdle)
	}
}
