package raft

import "sync"

type Channel struct {
	mu sync.Mutex
	ch chan interface{} // reflect
	closeFlag bool // not really close
}

func (channel *Channel) close() {
	channel.mu.Lock()
	if len(channel.ch) > 0 { <-channel.ch } // clean
	channel.closeFlag = true
	channel.mu.Unlock()
}

func (channel *Channel) open() {
	channel.mu.Lock()
	if len(channel.ch) > 0 { <-channel.ch} // clean
	channel.closeFlag = false
	channel.mu.Unlock()
}

func (channel *Channel) isClose() bool {
	channel.mu.Lock()
	defer channel.mu.Unlock()
	return channel.closeFlag
}