package main

import (
	"context"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	height int
	ts     time.Time
	tmRec  time.Time
	csRec  time.Time
}

type blocksCollector struct {
	heightBufferMap map[int]map[string]time.Time
	mux             sync.Mutex
	currentHeight   atomic.Int64
	statsCh         chan *Stats
	InfluxWriteAPI  api.WriteAPI
}

func NewBlocksCollector(influxWriteAPI api.WriteAPI) *blocksCollector {
	return &blocksCollector{
		heightBufferMap: make(map[int]map[string]time.Time),
		currentHeight:   atomic.Int64{},
		statsCh:         make(chan *Stats),
		InfluxWriteAPI:  influxWriteAPI,
	}
}

func (b *blocksCollector) collectStats(blockHeight int, blockTimestamp int, receivedAt int, source string) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if b.heightBufferMap[blockHeight] == nil {
		b.heightBufferMap[blockHeight] = make(map[string]time.Time)
	} else {
		// if we already have this block in buffer, we can send stats
		defer func() {
			b.currentHeight.Store(int64(blockHeight))
			b.statsCh <- &Stats{
				height: blockHeight,
				ts:     time.UnixMilli(int64(blockTimestamp)),
				tmRec:  b.heightBufferMap[blockHeight]["tm"],
				csRec:  b.heightBufferMap[blockHeight]["cs"],
			}
			delete(b.heightBufferMap, blockHeight)
		}()
	}

	b.heightBufferMap[blockHeight][source] = time.UnixMilli(int64(receivedAt))
}

func (b *blocksCollector) PushBlock(ctx context.Context, blockChannel chan *Block, source string) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-blockChannel:
			b.collectStats(event.BlockHeight, event.BlockTimestamp, event.ReceivedAt, source)
		}
	}
}
