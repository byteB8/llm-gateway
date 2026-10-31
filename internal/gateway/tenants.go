package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/byteB8/llm-gateway/internal/limit"
)

type Tenant struct {
	Name   string       `json:"name"`
	Keys   []string     `json:"keys"`
	Limits limit.Limits `json:"limits"`
}

// Tenants maps API keys to tenant names. A nil *Tenants means authentication is off.
type Tenants struct {
	list  []Tenant
	byKey map[string]string
}

func NewTenants(list []Tenant) (*Tenants, error) {
	t := &Tenants{list: list, byKey: map[string]string{}}
	for _, tn := range list {
		if tn.Name == "" {
			return nil, fmt.Errorf("tenant without a name")
		}
		for _, k := range tn.Keys {
			if prev, dup := t.byKey[k]; dup {
				return nil, fmt.Errorf("key used by both %q and %q", prev, tn.Name)
			}
			t.byKey[k] = tn.Name
		}
	}
	return t, nil
}

// LoadTenants reads {"tenants": [{"name", "keys", "limits"}]} from a JSON file.
func LoadTenants(path string) (*Tenants, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f struct {
		Tenants []Tenant `json:"tenants"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return NewTenants(f.Tenants)
}

// identify returns the request's tenant. Without a tenant list everyone is "anonymous".
func (t *Tenants) identify(r *http.Request) (string, bool) {
	if t == nil {
		return "anonymous", true
	}
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return "", false
	}
	name, ok := t.byKey[strings.TrimSpace(key)]
	return name, ok
}
