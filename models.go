package main

import "time"

type Block struct {
	ReceivedAt     time.Time `json:"received_at"`
	BlockHeight    int       `json:"block_height"`
	BlockTimestamp time.Time `json:"block_ts"`
}
