package gopool

import (
	"errors"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// ElemBase ...
type ElemBase interface {
	CreatedTime() time.Time
	SetInPool(bool)
	IsInPool() bool
}

// elemBase ...
type elemBase struct {
	created time.Time
	inpool  bool
}

// NewElemBase ...
func NewElemBase() ElemBase {
	return &elemBase{
		created: time.Now(),
	}
}

// CreatedTime ...
func (p *elemBase) CreatedTime() time.Time {
	return p.created
}

// SetInPool ...
func (p *elemBase) SetInPool(ok bool) {
	p.inpool = ok
}

// IsInPool ...
func (p *elemBase) IsInPool() bool {
	return p.inpool
}

// Elem ...
type Elem interface {
	Out() error
	IsAlive() bool
	ElemBase
}

// Pool ...
type Pool interface {
	Get() (elem Elem, err error)
	Put(elem Elem) (err error)
	Len() int
	Close() error
	IsClosed() bool
	OutAllBefore(time.Time)
}

// ChanPool ...
type ChanPool struct {
	factory    func(Pool) (Elem, error)
	elems      chan Elem
	closeChan  chan struct{}
	closed     int32
	heartTime  time.Duration
	maxWait    time.Duration
	maxActive  int32
	maxIdle    int32
	cur        int32
	status     int
	lastFailed int64
}

var _ Pool = (*ChanPool)(nil)

// NewChanPool ...
func NewChanPool(factory func(Pool) (Elem, error),
	maxActive, maxIdle int, heartTime, maxWait time.Duration) (cp *ChanPool) {
	cp = &ChanPool{
		factory:   factory,
		elems:     make(chan Elem, maxActive),
		heartTime: heartTime,
		maxActive: int32(maxActive),
		maxIdle:   int32(maxIdle),
		closeChan: make(chan struct{}),
	}
	go cp.run()
	return
}

// OutAllBefore ...
func (p *ChanPool) OutAllBefore(t time.Time) {
	atomic.StoreInt64(&p.lastFailed, t.Unix())
}

// IsClosed ...
func (p *ChanPool) IsClosed() bool {
	return atomic.LoadInt32(&p.closed) == 1
}

// Close ...
func (p *ChanPool) Close() error {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}
	close(p.closeChan)
	return nil
}

var (
	// ErrPoolClosed ...
	ErrPoolClosed = errors.New("pool closed")
)

// Get ...
func (p *ChanPool) Get() (elem Elem, err error) {
	if time.Now().Unix()-atomic.LoadInt64(&p.lastFailed) < int64(p.maxWait/time.Second) {
		err = os.ErrNotExist
		return
	}
	select {
	case elem = <-p.elems:
	case <-p.closeChan:
		err = ErrPoolClosed
	default:
		if atomic.AddInt32(&p.cur, 1) > p.maxActive {
			select {
			case elem = <-p.elems:
			case <-time.After(p.maxWait):
				err = os.ErrNotExist
				atomic.AddInt32(&p.cur, -1)
			}
		} else {
			elem, err = p.factory(p)
			if err != nil {
				atomic.AddInt32(&p.cur, -1)
			}
		}
	}
	if elem != nil {
		elem.SetInPool(false)
	}
	return
}

// Put ...
func (p *ChanPool) Put(elem Elem) (err error) {
	if elem.IsInPool() {
		log.Println("repeated put back")
		return
	}
	if elem.CreatedTime().Unix() <= atomic.LoadInt64(&p.lastFailed) {
		err = elem.Out()
		return
	}

	elem.SetInPool(true)
	if !p.IsClosed() {
		select {
		case p.elems <- elem:
			return
		case <-p.closeChan:
		default:
		}
	}

	return elem.Out()
}

func (p *ChanPool) run() {
	p.doCheck()
	for !p.IsClosed() {
		select {
		case <-p.closeChan:
			p.doStop()
			return
		case <-time.After(p.heartTime):
			p.doCheck()
		}
	}
}

func (p *ChanPool) doCheck() {
	var (
		elems      = p.elems
		n          = len(elems)
		now        = time.Now()
		lastFailed = atomic.LoadInt64(&p.lastFailed)
		first      Elem
		e          error
	)
	if now.Unix()-lastFailed < int64(p.maxWait/time.Second) {
		for i := 0; i < n; i++ {
			select {
			case elem := <-elems:
				if elem.CreatedTime().Unix() <= lastFailed {
					elem.Out()
					continue
				}
			}
		}
		n = len(elems)
	} else {
		first, e = p.Get()
		if e != nil {
			return
		}
		if !first.IsAlive() {
			first.Out()
		} else {
			p.Put(first)
		}
	}
	deleted := int32(0)
	maxDeleted := int32(n) - p.maxIdle
	if maxDeleted > 3 {
		maxDeleted /= 3
	}
	if maxDeleted > 0 {
		p.status++
	} else {
		p.status = 0
	}

	for i := 1; i < n; i++ {
		select {
		case elem := <-elems:
			elem.SetInPool(false)
			if elem == first {
				p.Put(elem)
				return
			}
			if deleted < maxDeleted && p.status > 3 && maxDeleted > 0 {
				elem.Out()
				deleted++
				break
			}
			if !elem.IsAlive() {
				elem.Out()
			} else {
				p.Put(elem)
			}
		case <-p.closeChan:
			return
		default:
			return
		}
	}
}

func (p *ChanPool) doStop() {
	for {
		select {
		case elem := <-p.elems:
			elem.Out()
		case <-time.After(time.Second * 3):
			return
		}
	}
}

// Len ...
func (p *ChanPool) Len() int {
	n := len(p.elems)
	return n
}
