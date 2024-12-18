package handler

import (
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
)

func newBeaconResponse(data any) *beaconhttp.BeaconResponse {
	return beaconhttp.NewBeaconResponse(data)
}
