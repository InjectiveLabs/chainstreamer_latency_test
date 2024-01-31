package main

type Block struct {
	ReceivedAt     int `json:"received_at"`
	BlockHeight    int `json:"block_height"`
	BlockTimestamp int `json:"block_ts"`
}
