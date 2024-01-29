package main

import (
	"context"
	"time"

	chainStreamModule "github.com/InjectiveLabs/sdk-go/chain/stream/types"
	"github.com/InjectiveLabs/sdk-go/client"
	chainclient "github.com/InjectiveLabs/sdk-go/client/chain"
	"github.com/InjectiveLabs/sdk-go/client/common"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func main() {
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

	btcUsdtPerpMarket := "0x4ca0f92fc28be0c9761326016b5a1a2177dd6375558365116b5bdda9abc229ce"

	req := chainStreamModule.StreamRequest{
		DerivativeOrderbooksFilter: &chainStreamModule.OrderbookFilter{
			MarketIds: []string{btcUsdtPerpMarket},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		if err := connectChainStream(ctx, chainClient, req, btcUsdtPerpMarket, logger); err != nil {
			logger.Errorln("Failed to maintain chain stream: ", err)
			time.Sleep(5 * time.Second) // Avoid given server to much stress
			continue
		} else {
			break
		}
	}
}

func connectChainStream(ctx context.Context, client chainclient.ChainClient, req chainStreamModule.StreamRequest, marketId string, logger *logrus.Logger) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel() // kill chain stream when ctx is done

	stream, err := client.ChainStream(streamCtx, req)
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

			orderbookUpdates := res.GetDerivativeOrderbookUpdates()

			orderbookUpdate := findOrderbookUpdateByMarketId(orderbookUpdates, marketId)
			if orderbookUpdate != nil {
				logger.Infof("Received stream message: blocktime %d, sequence number %d, ts %d", res.BlockTime, orderbookUpdate.Seq, time.Now().UnixMilli())
				if time.Since(time.UnixMilli(res.BlockTime)) > 10*time.Second {
					logger.Warnf("Received stream message is too old: blocktime %d, sequence number %d, ts %d", res.BlockTime, orderbookUpdate.Seq, time.Now().UnixMilli())
				}
			}
		}
	}
}

func findOrderbookUpdateByMarketId(orderbookUpdates []*chainStreamModule.OrderbookUpdate, marketId string) *chainStreamModule.OrderbookUpdate {
	for _, orderbookUpdate := range orderbookUpdates {
		if orderbookUpdate.Orderbook.MarketId == marketId {
			return orderbookUpdate
		}
	}

	return nil
}
