package models

import (
	"go-ride-kafka-consumers/internal/domain/location"

	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
)

type DriverLocationModel = schemamodels.DriverLocation

func FromDomainDriverLocation(e *location.DriverLocation) *DriverLocationModel {
	if e == nil {
		return nil
	}

	return &DriverLocationModel{
		ID:         e.ID,
		DriverID:   e.DriverID,
		Latitude:   e.Latitude,
		Longitude:  e.Longitude,
		Geohash:    e.Geohash,
		S2CellID:   e.S2CellID,
		RecordedAt: e.RecordedAt,
		CreatedAt:  e.CreatedAt,
		UpdatedAt:  e.UpdatedAt,
	}
}

func ToDomainDriverLocation(m *DriverLocationModel) *location.DriverLocation {
	if m == nil {
		return nil
	}

	return &location.DriverLocation{
		ID:         m.ID,
		DriverID:   m.DriverID,
		Latitude:   m.Latitude,
		Longitude:  m.Longitude,
		Geohash:    m.Geohash,
		S2CellID:   m.S2CellID,
		RecordedAt: m.RecordedAt,
		CreatedAt:  m.CreatedAt,
		UpdatedAt:  m.UpdatedAt,
	}
}
