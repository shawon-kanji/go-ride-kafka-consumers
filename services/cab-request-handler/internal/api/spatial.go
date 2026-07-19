package api

import (
	"strconv"

	"github.com/golang/geo/s2"
)

const (
	// s2GeohashLevel keeps a concise token while preserving neighborhood locality.
	s2GeohashLevel = 16
)

func geohashFromLatLng(latitude, longitude float64) string {
	cell := s2Cell(latitude, longitude).Parent(s2GeohashLevel)
	return cell.ToToken()
}

func s2CellIDFromLatLng(latitude, longitude float64) string {
	cell := s2Cell(latitude, longitude)
	return strconv.FormatUint(uint64(cell), 10)
}

func s2Cell(latitude, longitude float64) s2.CellID {
	latLng := s2.LatLngFromDegrees(latitude, longitude)
	return s2.CellIDFromLatLng(latLng)
}
