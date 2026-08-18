package directions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// ErrUnavailable covers every way a route lookup can fail — network error,
// non-OK HTTP status, non-OK API status, no key configured — since callers
// only ever do one thing with it: fall back to the haversine estimate.
var ErrUnavailable = errors.New("directions provider unavailable")

// Route is a real driving route between two points, from a directions
// provider — distinct from the haversine straight-line estimate used when
// a Route lookup fails.
type Route struct {
	DistanceKM      float64
	DurationMinutes float64
	Polyline        string
}

type Client interface {
	Route(ctx context.Context, originLat, originLng, destLat, destLng float64) (*Route, error)
}

const directionsURL = "https://maps.googleapis.com/maps/api/directions/json"

// GoogleClient calls the Directions API directly (no shared SDK dependency
// pulled in for one endpoint) — same approach as go-ride-backend's
// infrastructure/maps.GoogleClient for Places/Geocoding.
type GoogleClient struct {
	apiKey     string
	httpClient *http.Client
}

func NewGoogleClient(apiKey string) *GoogleClient {
	return &GoogleClient{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

type directionsAPIResponse struct {
	Status   string `json:"status"`
	ErrorMsg string `json:"error_message"`
	Routes   []struct {
		OverviewPolyline struct {
			Points string `json:"points"`
		} `json:"overview_polyline"`
		Legs []struct {
			Distance struct {
				Value float64 `json:"value"` // meters
			} `json:"distance"`
			Duration struct {
				Value float64 `json:"value"` // seconds
			} `json:"duration"`
		} `json:"legs"`
	} `json:"routes"`
}

func (c *GoogleClient) Route(ctx context.Context, originLat, originLng, destLat, destLng float64) (*Route, error) {
	if c.apiKey == "" {
		return nil, ErrUnavailable
	}

	q := url.Values{}
	q.Set("origin", fmt.Sprintf("%f,%f", originLat, originLng))
	q.Set("destination", fmt.Sprintf("%f,%f", destLat, destLng))
	q.Set("key", c.apiKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, directionsURL+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %v", ErrUnavailable, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: unexpected HTTP status %d", ErrUnavailable, resp.StatusCode)
	}

	var body directionsAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("%w: decode response: %v", ErrUnavailable, err)
	}

	if body.Status != "OK" {
		if body.ErrorMsg != "" {
			return nil, fmt.Errorf("%w: %s: %s", ErrUnavailable, body.Status, body.ErrorMsg)
		}
		return nil, fmt.Errorf("%w: %s", ErrUnavailable, body.Status)
	}
	if len(body.Routes) == 0 || len(body.Routes[0].Legs) == 0 {
		return nil, fmt.Errorf("%w: no route legs in response", ErrUnavailable)
	}

	leg := body.Routes[0].Legs[0]
	return &Route{
		DistanceKM:      leg.Distance.Value / 1000,
		DurationMinutes: leg.Duration.Value / 60,
		Polyline:        body.Routes[0].OverviewPolyline.Points,
	}, nil
}
