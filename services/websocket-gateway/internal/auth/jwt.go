package auth

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// Claims mirrors go-ride-backend/infrastructure/security/jwt.go's Claims
// shape exactly, since this gateway only ever verifies tokens minted there —
// it never generates one itself.
type Claims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

const (
	DriverRole = "driver"
	RiderRole  = "rider"
)

type Verifier struct {
	secretKey []byte
	issuer    string
}

func NewVerifier(secret, issuer string) *Verifier {
	return &Verifier{secretKey: []byte(secret), issuer: issuer}
}

// Parse verifies everything except audience up front, then checks the caller-
// supplied expectedAudience — this gateway serves two routes (driver and
// rider WebSocket upgrades) that must each only accept tokens minted for
// them, so a single fixed audience baked into the Verifier can't be correct
// for both; the caller (one per route) decides which audience applies.
func (v *Verifier) Parse(tokenValue, expectedAudience string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenValue, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return v.secretKey, nil
	}, jwt.WithIssuer(v.issuer), jwt.WithAudience(expectedAudience), jwt.WithExpirationRequired())
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	return claims, nil
}
