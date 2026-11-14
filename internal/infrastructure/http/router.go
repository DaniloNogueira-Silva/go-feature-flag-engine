package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/danil/feature-flag-engine/internal/application"
	"github.com/danil/feature-flag-engine/internal/domain/environment"
	"github.com/danil/feature-flag-engine/internal/domain/flag"
)

// Handler exposes the Control Plane REST API endpoints.
type Handler struct {
	svc *application.ControlPlaneService
}

// NewRouter constructs an http.Handler with all Control Plane routes registered.
func NewRouter(svc *application.ControlPlaneService) http.Handler {
	h := &Handler{svc: svc}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", h.handleHealth)
	mux.HandleFunc("POST /api/v1/environments", h.handleCreateEnvironment)
	mux.HandleFunc("GET /api/v1/environments", h.handleListEnvironments)
	mux.HandleFunc("POST /api/v1/flags", h.handleCreateFlag)
	mux.HandleFunc("GET /api/v1/flags", h.handleListFlags)
	mux.HandleFunc("PUT /api/v1/environments/{envKey}/flags/{flagKey}", h.handleUpsertFlagConfig)
	mux.HandleFunc("GET /api/v1/environments/{envKey}/flags/{flagKey}", h.handleGetFlagConfig)
	mux.HandleFunc("GET /api/v1/environments/{envKey}/snapshot", h.handleGetSnapshot)
	mux.HandleFunc("POST /api/v1/environments/{envKey}/flags/{flagKey}/evaluate", h.handleEvaluateFlag)

	return withCORS(mux)
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"component": "control-plane",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *Handler) handleCreateEnvironment(w http.ResponseWriter, r *http.Request) {
	var in application.CreateEnvironmentInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	env, err := h.svc.CreateEnvironment(r.Context(), in)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, env)
}

func (h *Handler) handleListEnvironments(w http.ResponseWriter, r *http.Request) {
	envs, err := h.svc.ListEnvironments(r.Context())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"environments": envs})
}

func (h *Handler) handleCreateFlag(w http.ResponseWriter, r *http.Request) {
	var in application.CreateFlagInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	f, err := h.svc.CreateFlag(r.Context(), in)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, f)
}

func (h *Handler) handleListFlags(w http.ResponseWriter, r *http.Request) {
	flags, err := h.svc.ListFlags(r.Context())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flags": flags})
}

func (h *Handler) handleUpsertFlagConfig(w http.ResponseWriter, r *http.Request) {
	envKey := r.PathValue("envKey")
	flagKey := r.PathValue("flagKey")

	var in application.UpsertFlagConfigInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	in.EnvironmentKey = envKey
	in.FlagKey = flagKey

	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && in.ExpectedVersion == nil {
		if v, err := strconv.ParseInt(ifMatch, 10, 64); err == nil {
			in.ExpectedVersion = &v
		}
	}

	cfg, err := h.svc.UpsertFlagConfig(r.Context(), in)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (h *Handler) handleGetFlagConfig(w http.ResponseWriter, r *http.Request) {
	envKey := r.PathValue("envKey")
	flagKey := r.PathValue("flagKey")

	cfg, err := h.svc.GetFlagConfig(r.Context(), envKey, flagKey)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (h *Handler) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	envKey := r.PathValue("envKey")
	snap, err := h.svc.GetEnvironmentSnapshot(r.Context(), envKey)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleEvaluateFlag is a diagnostic Playground endpoint on the Control Plane
// to test how a flag evaluates for a sample user context.
func (h *Handler) handleEvaluateFlag(w http.ResponseWriter, r *http.Request) {
	envKey := r.PathValue("envKey")
	flagKey := r.PathValue("flagKey")

	var req struct {
		UserID     string            `json:"user_id"`
		Attributes map[string]string `json:"attributes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	cfg, err := h.svc.GetFlagConfig(r.Context(), envKey, flagKey)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	compiled, err := cfg.Compile()
	if err != nil {
		writeDomainError(w, err)
		return
	}
	res := compiled.Evaluate(flag.EvaluationContext{
		UserID:     req.UserID,
		Attributes: req.Attributes,
	})
	writeJSON(w, http.StatusOK, res)
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, flag.ErrFlagNotFound), errors.Is(err, environment.ErrEnvironmentNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, flag.ErrVersionConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, flag.ErrInvalidFlagKey), errors.Is(err, flag.ErrInvalidRollout), errors.Is(err, environment.ErrInvalidEnvKey):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, If-Match")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

