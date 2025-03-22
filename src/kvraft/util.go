package kvraft

import "time"
import random "math/rand"
import "math/big"
import "crypto/rand"

func timeout(start int64, end int64) time.Duration {
	ms := start + (random.Int63() % (end - start)) // ms [start, end]
	return time.Duration(ms) * time.Millisecond
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}