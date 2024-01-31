package main

import (
	"context"
	"flag"
	"github.com/cometbft/cometbft/types"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"strings"
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

var tm_address string
var cs_address string
var Influx_token string
var Influx_url string
var Influx_org string
var Influx_bucket string

func init() {
	flag.StringVar(&tm_address, "tm_address", "https://sentry.tm.injective.network:443", "tendermint address address endpoint")
	flag.StringVar(&cs_address, "cs_address", "https://sentry.cs.injective.network:443", "chainstreamer address endpoint")
	flag.StringVar(&Influx_token, "influx_token", "", "influx token")
	flag.StringVar(&Influx_url, "influx_url", "", "influx url")
	flag.StringVar(&Influx_org, "influx_org", "", "influx org")
	flag.StringVar(&Influx_bucket, "influx_bucket", "", "influx bucket")
	flag.Parse()
}

func main() {

	// print vars
	logrus.Infof("tm_address: %s", tm_address)
	logrus.Infof("cs_address: %s", cs_address)
	logrus.Infof("influx_url: %s", Influx_url)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer func() {
		cancel()
	}()

	logger := logrus.New() // You can decide if you want to output to stdout or file or both here.
	sentry := "lb"

	network := common.NewNetwork()
	network.ChainStreamGrpcEndpoint = cs_address
	if strings.Contains(tm_address, "sentry") {
		network = common.LoadNetwork("mainnet", sentry)
	}
	clientCtx, err := chainclient.NewClientContext(
		"injective-1",
		"",
		nil,
	)
	if err != nil {
		panic(err)
	}
	clientCtx = clientCtx.WithNodeURI(cs_address)

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

	collector := NewBlocksCollector(influxWriteAPI)

	tmBlocksChannel := make(chan *Block)
	g.Go(func() error {
		return tmBlockReceives(ctx, tmBlocksChannel, influxWriteAPI)
	})

	chainstreamerBlocksCh := make(chan *Block)
	g.Go(func() error {
		return chainStreamBlockReceives(ctx, chainClient, chainstreamerBlocksCh, influxWriteAPI)
	})

	g.Go(func() error {
		return collector.PushBlock(ctx, tmBlocksChannel, "tm")
	})
	g.Go(func() error {
		return collector.PushBlock(ctx, chainstreamerBlocksCh, "cs")
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
					p := influxdb2.NewPointWithMeasurement("streamer_lat")
					p = p.AddField("tm_lat_ms", tmLat)
					p = p.AddField("cs_lat_ms", stLat)
					p = p.AddField("height", event.height)
					p = p.SetTime(event.ts)
					influxWriteAPI.WritePoint(p)
				}
			}
		}
	})

	err = g.Wait()
	if err != nil {
		panic(err)
	}
	influxClient.Close()
}

func chainStreamBlockReceives(ctx context.Context, client chainclient.ChainClient, blockCh chan<- *Block, influxWriteAPI api.WriteAPI) error {

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

	healthCheckTime := 5 * time.Second
	ticker := time.NewTicker(healthCheckTime)
	defer ticker.Stop()
	lastRecBlock := &Block{}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			res, err := stream.Recv()
			if err != nil {
				return errors.Wrap(err, "failed to receive stream message")
			}
			ticker.Reset(5 * time.Second)
			hb := &Block{
				BlockHeight:    int(res.BlockHeight),
				BlockTimestamp: int(res.BlockTime),
				ReceivedAt:     int(time.Now().UnixMilli()),
			}
			lastRecBlock = hb
			blockCh <- hb
		case t := <-ticker.C:
			ticker.Reset(1 * time.Second)
			lastRecBlockTime := time.UnixMilli(int64(lastRecBlock.ReceivedAt))
			logrus.Warnf("elapsed time since last received block from  %.2fs: %s", t.Sub(lastRecBlockTime).Seconds(), "tm")
			collectStreamerStats(t.Sub(lastRecBlockTime), influxWriteAPI, "cs")
		}
	}
}

func tmBlockReceives(ctx context.Context, blockCh chan<- *Block, influxWriteAPI api.WriteAPI) (err error) {
	cometBftClient, err := rpchttp.New(tm_address, "/websocket")
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

	healthCheckTime := 5 * time.Second
	ticker := time.NewTicker(healthCheckTime)
	defer ticker.Stop()

	lastRecBlock := &Block{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-eventCh:
			ticker.Reset(5 * time.Second)
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
			lastRecBlock = hb
			blockCh <- hb
		case t := <-ticker.C:
			ticker.Reset(1 * time.Second)
			lastRecBlockTime := time.UnixMilli(int64(lastRecBlock.ReceivedAt))
			logrus.Warnf("elapsed time since last received block from  %.2fs: %s", t.Sub(lastRecBlockTime).Seconds(), "tm")
			collectStreamerStats(t.Sub(lastRecBlockTime), influxWriteAPI, "tm")
		}
	}
}

func collectStreamerStats(elapsedTimeSinceLastReceivedBlock time.Duration, influxWriteAPI api.WriteAPI, source string) {
	if influxWriteAPI != nil && elapsedTimeSinceLastReceivedBlock.Milliseconds() > 0 {
		p := influxdb2.NewPointWithMeasurement("streamer_stats")
		p = p.AddField("elapsed_ms_since_last_received_block", elapsedTimeSinceLastReceivedBlock.Milliseconds())
		p = p.AddTag("source", source)
		p = p.SetTime(time.Now())
		influxWriteAPI.WritePoint(p)
	}
}
