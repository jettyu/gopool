package gopool

import (
	"errors"
	"os"
	"sync/atomic"
	"time"
)

// Elem ...
type Elem interface {
	Out() error
	IsAlive() bool
}

// Pool ...
type Pool interface {
	Get() (elem Elem, err error)
	Put(elem Elem) (err error)
	Len() int
	Close() error
	IsClosed() bool
}

// ChanPool ...
type ChanPool struct {
	factory   func(Pool) (Elem, error)
	elems     chan Elem
	closeChan chan struct{}
	closed    int32
	heartTime time.Duration
	maxWait   time.Duration
	maxActive int32
	maxIdle   int32
	cur       int32
	status    int
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
	return
}

// Put ...
func (p *ChanPool) Put(elem Elem) (err error) {
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
	elems := p.elems
	n := len(elems)
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
	var first Elem
	select {
	case first = <-elems:
		if !first.IsAlive() {
			first.Out()
		} else {
			p.Put(first)
		}
	default:
		return
	}

	for i := 1; i < n; i++ {
		select {
		case elem := <-elems:
			if elem == first {
				p.Put(elem)
				return
			}
			if deleted < maxDeleted && p.status > 3 && maxDeleted > 0 {
				elem.Out()
				deleted++
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
