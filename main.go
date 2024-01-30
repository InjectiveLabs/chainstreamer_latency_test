package main

import (
	"context"
	"flag"
	"github.com/cometbft/cometbft/types"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	chainStreamModule "github.com/InjectiveLabs/sdk-go/chain/stream/types"
	"github.com/InjectiveLabs/sdk-go/client"
	chainclient "github.com/InjectiveLabs/sdk-go/client/chain"
	"github.com/InjectiveLabs/sdk-go/client/common"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

var Influx_token string
var Influx_url string
var Influx_org string
var Influx_bucket string

func init() {
	flag.StringVar(&Influx_token, "influx_token", "", "influx token")
	flag.StringVar(&Influx_url, "influx_url", "", "influx url")
	flag.StringVar(&Influx_org, "influx_org", "", "influx org")
	flag.StringVar(&Influx_bucket, "influx_bucket", "", "influx bucket")
	flag.Parse()
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer func() {
		cancel()
	}()

	logger := logrus.New() // You can decide if you want to output to stdout or file or both here.
	sentry := "lb"

	network := common.LoadNetwork("mainnet", sentry)

	clientCtx, err := chainclient.NewClientContext(
		network.ChainId,
		"",
		nil,
	)
	if err != nil {
		panic(err)
	}
	clientCtx = clientCtx.WithNodeURI(network.TmEndpoint)

	chainClient, err := chainclient.NewChainClient(
		clientCtx,
		network,
		common.OptionGasPrices(client.DefaultGasPriceWithDenom),
	)
	if err != nil {
		panic(err)
	}

	g, ctx := errgroup.WithContext(ctx)

	var influxWriteAPI api.WriteAPI
	var influxClient influxdb2.Client
	if Influx_url != "" {
		influxClient = influxdb2.NewClient(Influx_url, Influx_token)
		influxWriteAPI = influxClient.WriteAPI(Influx_org, Influx_bucket)
		errorsCh := influxWriteAPI.Errors()
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case err = <-errorsCh:
					return err
				}
			}
		})
	}
	collector := NewBlocksCollector()

	tmBlocksChannel := make(chan *Block)

	g.Go(func() error {
		return tmBlockReceives(ctx, tmBlocksChannel)
	})

	chainstreamerBlocksCh := make(chan *Block)
	g.Go(func() error {
		return chainStreamBlockReceives(ctx, chainClient, chainstreamerBlocksCh)
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case event := <-tmBlocksChannel:
				collector.pushToMap(event.BlockHeight, event.BlockTimestamp, event.ReceivedAt, "tm")
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case event := <-chainstreamerBlocksCh:
				collector.pushToMap(event.BlockHeight, event.BlockTimestamp, event.ReceivedAt, "cs")
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case event := <-collector.statsCh:
				format := `Jan 02 15:04:05.000ms`

				stLat := event.csRec.Sub(event.ts).Milliseconds()
				tmLat := event.tmRec.Sub(event.ts).Milliseconds()
				logger.WithFields(logrus.Fields{
					"block ts": event.ts.Format(format),
					/*"tm": event.tmRec.Format(format),
					"cs": event.csRec.Format(format),*/
					//ts latency
					"chain latency ms":    tmLat,
					"streamer latency ms": stLat,
					"height":              event.height,
				}).Infoln("stats")
				if influxWriteAPI != nil {
					p := influxdb2.NewPointWithMeasurement("streamer_stats")
					p = p.AddField("chain_latency_ms", tmLat)
					p = p.AddField("streamer_latency_ms", stLat)
					p = p.AddField("height", event.height)
					p = p.SetTime(event.ts)
					influxWriteAPI.WritePoint(p)
				}
			}
		}
	})

	g.Wait()
	influxClient.Close()
}

func chainStreamBlockReceives(ctx context.Context, client chainclient.ChainClient, blockCh chan<- *Block) error {

	btcUsdtPerpMarket := "0x4ca0f92fc28be0c9761326016b5a1a2177dd6375558365116b5bdda9abc229ce"

	req := chainStreamModule.StreamRequest{
		DerivativeOrderbooksFilter: &chainStreamModule.OrderbookFilter{
			MarketIds: []string{btcUsdtPerpMarket},
		},
	}
	stream, err := client.ChainStream(ctx, req)
	if err != nil {
		return errors.Wrap(err, "failed to create chain stream")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			res, err := stream.Recv()
			if err != nil {
				return errors.Wrap(err, "failed to receive stream message")
			}
			hb := &Block{
				BlockHeight:    int(res.BlockHeight),
				BlockTimestamp: int(res.BlockTime),
				ReceivedAt:     int(time.Now().UnixMilli()),
			}
			blockCh <- hb
		}
	}
}

func tmBlockReceives(ctx context.Context, blockCh chan<- *Block) (err error) {
	tmEndpoint := "https://sentry.tm.injective.network:443"
	cometBftClient, err := rpchttp.New(tmEndpoint, "/websocket")
	if err != nil {
		return err
	}
	if !cometBftClient.IsRunning() {
		err = cometBftClient.Start()
		if err != nil {
			return err
		}
	}

	query := `tm.event='NewBlockHeader'`
	eventCh, err := cometBftClient.Subscribe(context.Background(), "", query, 10000)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-eventCh:
			if event.Data == nil {
				continue
			}

			var blockHeight int
			var blockTimestamp int
			blockHeight = int(event.Data.(types.EventDataNewBlockHeader).Header.Height)
			blockTimestamp = int(event.Data.(types.EventDataNewBlockHeader).Header.Time.UnixMilli())
			hb := &Block{
				BlockHeight:    blockHeight,
				BlockTimestamp: blockTimestamp,
				ReceivedAt:     int(time.Now().UnixMilli()),
			}
			blockCh <- hb
		}
	}
}

type Block struct {
	ReceivedAt     int `json:"received_at"`
	BlockHeight    int `json:"block_height"`
	BlockTimestamp int `json:"block_ts"`
}

type blocksCollector struct {
	heightBufferMap map[int]map[string]time.Time
	mux             sync.Mutex
	currentHeight   atomic.Int64
	statsCh         chan *Stats
}

func NewBlocksCollector() *blocksCollector {
	return &blocksCollector{
		heightBufferMap: make(map[int]map[string]time.Time),
		currentHeight:   atomic.Int64{},
		statsCh:         make(chan *Stats),
	}
}

type Stats struct {
	height int
	ts     time.Time
	tmRec  time.Time
	csRec  time.Time
}

func (b *blocksCollector) pushToMap(blockHeight int, blockTimestamp int, receivedAt int, source string) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if b.heightBufferMap[blockHeight] == nil {
		b.heightBufferMap[blockHeight] = make(map[string]time.Time)
	} else {
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
