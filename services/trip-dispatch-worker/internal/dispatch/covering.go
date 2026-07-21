package dispatch

import (
	"strconv"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

const (
	// earthRadiusKM matches the constant used by the SQL Haversine expression
	// in nearestDriversQuery, so the S2 covering and the exact distance filter
	// agree on the same radius definition.
	earthRadiusKM = 6371.0

	// s2CoveringMaxCells bounds the number of cells RegionCoverer will use to
	// approximate the search disc; this matches the library's own default.
	s2CoveringMaxCells = 8
)

// s2CoveringRanges computes an S2 covering for a disc of radiusKM around
// (lat, lng) and returns parallel slices of decimal-string leaf-level cell ID
// bounds (RangeMin/RangeMax) for each covering cell. These bounds are used as
// `s2_cell_id BETWEEN rangeMin AND rangeMax` pre-filters against
// driver_locations, which stores each driver's exact (leaf-level) S2 cell ID.
// The covering is an over-approximation of the disc, so an exact distance
// filter must still be applied to any rows it selects.
func s2CoveringRanges(lat, lng, radiusKM float64) (mins, maxs []string) {
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	angle := s1.Angle(radiusKM / earthRadiusKM)
	cap := s2.CapFromCenterAngle(center, angle)

	coverer := &s2.RegionCoverer{MinLevel: 0, MaxLevel: s2.MaxLevel, MaxCells: s2CoveringMaxCells}
	covering := coverer.Covering(cap)

	mins = make([]string, len(covering))
	maxs = make([]string, len(covering))
	for i, cellID := range covering {
		mins[i] = strconv.FormatUint(uint64(cellID.RangeMin()), 10)
		maxs[i] = strconv.FormatUint(uint64(cellID.RangeMax()), 10)
	}
	return mins, maxs
}
