package dispatch

import (
	"testing"
	"time"

	"github.com/google/uuid"
	schemamodels "github.com/shawon-kanji/go-ride-db-schema/models"
)

func TestBuildJobOfferEventCarriesRiderTripAndPerDriverPickupETA(t *testing.T) {
	driverA := uuid.New()
	driverB := uuid.New()
	now := time.Now().UTC()

	req := schemamodels.TripRequest{
		ID:         uuid.New(),
		TripID:     uuid.New(),
		RiderID:    uuid.New(),
		PickupLat:  1.35,
		PickupLng:  103.87,
		DropoffLat: 1.28,
		DropoffLng: 103.85,
	}

	offers := []schemamodels.DriverJobOffer{
		{ID: uuid.New(), DriverID: driverA, OfferRank: 0, OfferedAt: now, ExpiresAt: now.Add(15 * time.Second)},
		{ID: uuid.New(), DriverID: driverB, OfferRank: 1, OfferedAt: now, ExpiresAt: now.Add(15 * time.Second)},
	}

	routeDistance := 12.0
	routeDuration := 20.0
	fare := &schemamodels.TripFare{
		TotalFare:            100,
		CurrencyCode:         "SGD",
		RouteDistanceKM:      &routeDistance,
		RouteDurationMinutes: &routeDuration,
	}

	pickupDistances := map[uuid.UUID]float64{
		driverA: 3.0,
		driverB: 6.0,
	}

	event := buildJobOfferEvent(req, offers, fare, "Ada Lovelace", pickupDistances, 0.20, 30)

	if event.RiderName != "Ada Lovelace" {
		t.Fatalf("rider name = %q, want %q", event.RiderName, "Ada Lovelace")
	}
	if event.TripDistanceKM == nil || *event.TripDistanceKM != routeDistance {
		t.Fatalf("trip distance = %v, want %v", event.TripDistanceKM, routeDistance)
	}
	if event.TripDurationMinutes == nil || *event.TripDurationMinutes != routeDuration {
		t.Fatalf("trip duration = %v, want %v", event.TripDurationMinutes, routeDuration)
	}
	if event.EstimatedEarning != 80 {
		t.Fatalf("estimated earning = %v, want 80", event.EstimatedEarning)
	}

	if len(event.Offers) != 2 {
		t.Fatalf("offer count = %d, want 2", len(event.Offers))
	}

	byDriver := map[string]float64{}
	for _, entry := range event.Offers {
		byDriver[entry.DriverID] = entry.PickupETAMinutes
		if entry.DriverID == driverA.String() && entry.PickupDistanceKM != 3.0 {
			t.Fatalf("driver A pickup distance = %v, want 3.0", entry.PickupDistanceKM)
		}
		if entry.DriverID == driverB.String() && entry.PickupDistanceKM != 6.0 {
			t.Fatalf("driver B pickup distance = %v, want 6.0", entry.PickupDistanceKM)
		}
	}

	// driverA is closer (3km vs 6km at 30 KPH), so must get a smaller ETA.
	if byDriver[driverA.String()] >= byDriver[driverB.String()] {
		t.Fatalf("expected driver A's pickup ETA (%v) to be less than driver B's (%v)", byDriver[driverA.String()], byDriver[driverB.String()])
	}
	if got, want := byDriver[driverA.String()], 6.0; got != want {
		t.Fatalf("driver A pickup ETA minutes = %v, want %v (3km at 30 KPH)", got, want)
	}
}

func TestBuildJobOfferEventMissingCandidateGetsZeroDistanceNotPanic(t *testing.T) {
	req := schemamodels.TripRequest{ID: uuid.New(), TripID: uuid.New(), RiderID: uuid.New()}
	offer := schemamodels.DriverJobOffer{ID: uuid.New(), DriverID: uuid.New(), OfferRank: 0}

	event := buildJobOfferEvent(req, []schemamodels.DriverJobOffer{offer}, nil, "", map[uuid.UUID]float64{}, 0.2, 30)

	if len(event.Offers) != 1 {
		t.Fatalf("offer count = %d, want 1", len(event.Offers))
	}
	if event.Offers[0].PickupDistanceKM != 0 || event.Offers[0].PickupETAMinutes != 0 {
		t.Fatalf("expected zero pickup distance/ETA for missing candidate, got %+v", event.Offers[0])
	}
	if event.TripDistanceKM != nil || event.TripDurationMinutes != nil {
		t.Fatalf("expected nil trip route when fare is nil, got %+v/%+v", event.TripDistanceKM, event.TripDurationMinutes)
	}
}
