package devvalidator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPostAndDecode_AttesterDuties verifies that postAndDecode correctly
// sends a POST with a JSON body and decodes the response's "data" field.
// This is a regression test for the bug where maybeAttest used post()
// (which discards the response) followed by get() (which hits a
// non-existent GET endpoint → 405).
func TestPostAndDecode_AttesterDuties(t *testing.T) {
	type duty struct {
		ValidatorIndex string `json:"validator_index"`
		Slot           string `json:"slot"`
	}

	expectedDuties := []duty{
		{ValidatorIndex: "0", Slot: "5"},
		{ValidatorIndex: "1", Slot: "5"},
	}

	// Mock beacon server: POST-only attester duties endpoint.
	mux := http.NewServeMux()
	mux.HandleFunc("/eth/v1/validator/duties/attester/0", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Verify we received valid JSON indices in the body.
		var indices []string
		if err := json.NewDecoder(r.Body).Decode(&indices); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		require.NotEmpty(t, indices)

		// Return duties wrapped in standard Beacon API response.
		resp := map[string]interface{}{"data": expectedDuties}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewBeaconClient(srv.URL)
	ctx := context.Background()

	// postAndDecode should succeed and parse the duties.
	var duties []duty
	err := client.postAndDecode(ctx, "/eth/v1/validator/duties/attester/0", []string{"0", "1"}, &duties)
	require.NoError(t, err)
	require.Len(t, duties, 2)
	require.Equal(t, "0", duties[0].ValidatorIndex)
	require.Equal(t, "5", duties[0].Slot)
}

// TestGet_AttesterDuties_Fails confirms that a GET request to a POST-only
// endpoint returns an error. This documents the original bug.
func TestGet_AttesterDuties_Fails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/eth/v1/validator/duties/attester/0", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		resp := map[string]interface{}{"data": []interface{}{}}
		json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewBeaconClient(srv.URL)
	ctx := context.Background()

	// GET must fail with 405 — this is the bug that postAndDecode fixes.
	var duties []interface{}
	err := client.get(ctx, "/eth/v1/validator/duties/attester/0", &duties)
	require.Error(t, err)
	require.Contains(t, err.Error(), "405")
}
