package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danil/feature-flag-engine/internal/application"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
	ffhttp "github.com/danil/feature-flag-engine/internal/infrastructure/http"
	"github.com/danil/feature-flag-engine/internal/infrastructure/memory"
)

func TestControlPlaneRESTFlow(t *testing.T) {
	t.Parallel()

	store := memory.NewStore()
	svc := application.NewControlPlaneService(store, store, store)
	srv := httptest.NewServer(ffhttp.NewRouter(svc))
	defer srv.Close()

	// 1. Create a new flag
	createFlagBody := []byte(`{
		"key": "one-click-checkout",
		"name": "One Click Checkout",
		"description": "Enables instant checkout for verified users"
	}`)
	resp, err := http.Post(srv.URL+"/api/v1/flags", "application/json", bytes.NewReader(createFlagBody))
	if err != nil {
		t.Fatalf("POST /api/v1/flags failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	// 2. Update targeting rules in production
	updateBody := []byte(`{
		"enabled": true,
		"default_rollout_basis_points": 0,
		"targeting_rules": [
			{
				"id": "rule-malta-100",
				"name": "100% for Malta users",
				"clauses": [
					{
						"attribute": "country",
						"operator": "IN",
						"values": ["MT", "BR"]
					}
				],
				"rollout_basis_points": 10000
			}
		]
	}`)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/api/v1/environments/production/flags/one-click-checkout", bytes.NewReader(updateBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", putResp.StatusCode)
	}

	var updatedCfg flag.FlagConfig
	if err := json.NewDecoder(putResp.Body).Decode(&updatedCfg); err != nil {
		t.Fatal(err)
	}
	if updatedCfg.Version != 2 { // 1 on initial create + 1 on PUT
		t.Fatalf("Version = %d, want 2", updatedCfg.Version)
	}

	// 3. Test optimistic concurrency conflict with stale If-Match
	conflictReq, _ := http.NewRequest(http.MethodPut, srv.URL+"/api/v1/environments/production/flags/one-click-checkout", bytes.NewReader(updateBody))
	conflictReq.Header.Set("Content-Type", "application/json")
	conflictReq.Header.Set("If-Match", "1")
	conflictResp, err := http.DefaultClient.Do(conflictReq)
	if err != nil {
		t.Fatal(err)
	}
	defer conflictResp.Body.Close()
	if conflictResp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", conflictResp.StatusCode)
	}

	// 4. Fetch environment snapshot
	snapResp, err := http.Get(srv.URL + "/api/v1/environments/production/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer snapResp.Body.Close()
	var snap application.EnvironmentSnapshot
	if err := json.NewDecoder(snapResp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Flags) != 1 || snap.Flags[0].FlagKey != "one-click-checkout" {
		t.Fatalf("unexpected snapshot flags: %+v", snap.Flags)
	}
}

