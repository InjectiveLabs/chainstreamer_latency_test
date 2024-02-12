package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/InjectiveLabs/injective-core/injective-chain/opentelemetry"
	core_types "github.com/InjectiveLabs/injective-core/injective-chain/stream/types"

	"github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/types"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

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

	shutdownOtel, err := opentelemetry.SetupOTelSDK(ctx)
	if err != nil {
		defer shutdownOtel(ctx)
	}

	cc, err := grpc.Dial(cs_address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Errorf("failed to init chainstreamer client: %v", err))
	}

	// nolint:staticcheck //ignored on purpose
	defer cc.Close()
	cs_client := core_types.NewStreamClient(cc)

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

					return fmt.Errorf("failed to write to influx: %w", err)
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
		return chainStreamBlockReceives(ctx, cs_client, chainstreamerBlocksCh, influxWriteAPI)
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

func chainStreamBlockReceives(ctx context.Context, client core_types.StreamClient, blockCh chan<- *Block, influxWriteAPI api.WriteAPI) error {

	btcUsdtPerpMarket := "0x4ca0f92fc28be0c9761326016b5a1a2177dd6375558365116b5bdda9abc229ce"

	req := core_types.StreamRequest{
		DerivativeOrderbooksFilter: &core_types.OrderbookFilter{
			MarketIds: []string{btcUsdtPerpMarket},
		},
	}
	stream, err := client.Stream(ctx, &req)
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

			recSpanCtx := trace.SpanContext{}
			if t, ok := res.Metadata["spanID"]; ok {
				spanID, err := trace.SpanIDFromHex(t)
				if err != nil {
					panic(err)
				}
				traceID, err := trace.TraceIDFromHex(res.Metadata["traceID"])
				b, _ := strconv.Atoi(res.Metadata["traceFlags"])
				traceFlags := trace.TraceFlags(byte(b))
				recSpanCtx = recSpanCtx.WithSpanID(spanID).WithTraceID(traceID).WithTraceFlags(traceFlags)
			}

			ctx1 := trace.ContextWithRemoteSpanContext(stream.Context(), recSpanCtx)

			_, span := opentelemetry.Tracer.Start(ctx1, "client_block_processing")
			span.SetAttributes(attribute.String("client_ip", GetLocalIP()))
			span.SetAttributes(attribute.String("block_height", fmt.Sprint(res.BlockHeight)))

			hb := &Block{
				BlockHeight:    int(res.BlockHeight),
				BlockTimestamp: int(res.BlockTime),
				ReceivedAt:     int(time.Now().UnixMilli()),
			}
			bz, _ := json.Marshal(res)
			span.AddEvent("client_msg_marshalled", trace.WithAttributes(
				attribute.String("msg_size", fmt.Sprint(len(bz))),
			))

			span.End()

			lastRecBlock = hb
			blockCh <- hb
		case t := <-ticker.C:
			ticker.Reset(1 * time.Second)
			lastRecBlockTime := time.UnixMilli(int64(lastRecBlock.BlockTimestamp))
			elapsedTimeSinceLastReceivedBlock := t.Sub(lastRecBlockTime)
			logrus.Warnf("elapsed time since last received block from  %.2fs: %s", elapsedTimeSinceLastReceivedBlock.Seconds(), "cs")
			if elapsedTimeSinceLastReceivedBlock.Milliseconds() <= 0 {
				logrus.Infof("block height: %d, block ts: %s, elapsed time ms: %d, source: %s", lastRecBlock.BlockHeight, lastRecBlockTime, elapsedTimeSinceLastReceivedBlock.Milliseconds(), "cs")
			}

			collectStreamerStats(t.Sub(lastRecBlockTime), influxWriteAPI, "cs")
		}
	}
}

func tmBlockReceives(ctx context.Context, blockCh chan<- *Block, influxWriteAPI api.WriteAPI) (err error) {
	cometBftClient, err := rpchttp.New(tm_address, "/websocket")
	if err != nil {
		return fmt.Errorf("failed to create comet bft client: %w", err)
	}
	if !cometBftClient.IsRunning() {
		err = cometBftClient.Start()
		if err != nil {
			return fmt.Errorf("failed to start comet bft client: %w", err)
		}
	}

	query := `tm.event='NewBlockHeader'`
	eventCh, err := cometBftClient.Subscribe(context.Background(), "", query, 10000)
	if err != nil {
		return fmt.Errorf("failed to subscribe to comet bft client: %w", err)
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

			/*randVal := time.Duration(rand.Intn(8)) * time.Second
			time.Sleep(randVal)*/

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
			lastRecBlockTime := time.UnixMilli(int64(lastRecBlock.BlockTimestamp))
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

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
