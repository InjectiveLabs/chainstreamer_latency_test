package main

import (
	"context"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	height int
	ts     time.Time
	// received at time
	tmRec time.Time
	// received at time
	csRec time.Time
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

func (b *blocksCollector) PushBlock(ctx context.Context, blockChannel chan *Block, source string, influxWriteAPI api.WriteAPI) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-blockChannel:
			if influxWriteAPI != nil {
				lat := event.ReceivedAt.Sub(event.BlockTimestamp).Milliseconds()
				p := influxdb2.NewPointWithMeasurement("streamer_lat")
				p = p.AddField(fmt.Sprintf("%s_lat_ms", source), lat)
				p = p.AddField("height", event.BlockHeight)
				p = p.SetTime(time.Now())
				influxWriteAPI.WritePoint(p)
			}
		}
	}
}
