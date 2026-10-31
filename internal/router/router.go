// Package router decides which backend serves a request. Every policy returns all backends in
// preference order, so the gateway can fall through to the next one when a backend fails.
package router

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync/atomic"
)

type Backend struct {
	Name     string
	URL      string
	Capacity int64 // requests it serves at once (vLLM max_num_seqs); 0 means unknown
	inflight atomic.Int64
}

// Full reports whether another request would have to queue on the backend.
func (b *Backend) Full() bool { return b.Capacity > 0 && b.Inflight() >= b.Capacity }

func (b *Backend) Inflight() int64 { return b.inflight.Load() }
func (b *Backend) Acquire()        { b.inflight.Add(1) }
func (b *Backend) Release()        { b.inflight.Add(-1) }

// ParseBackends reads "name=url,name=url" (a bare url is named after its position).
func ParseBackends(spec string) ([]*Backend, error) {
	var out []*Backend
	for i, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, url, ok := strings.Cut(part, "=")
		if !ok {
			name, url = fmt.Sprintf("b%d", i), part
		}
		out = append(out, &Backend{Name: name, URL: strings.TrimRight(url, "/")})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no backends in %q", spec)
	}
	return out, nil
}

type Policy interface {
	// Order returns every backend, most preferred first. key is the request's prefix key.
	Order(key string) []*Backend
	Name() string
}

func New(name string, backends []*Backend, loadFactor float64) (Policy, error) {
	switch name {
	case "round-robin":
		return &RoundRobin{backends: backends}, nil
	case "least-loaded":
		return &LeastLoaded{backends: backends}, nil
	case "prefix-affinity":
		return NewPrefixAffinity(backends, loadFactor), nil
	}
	return nil, fmt.Errorf("unknown policy %q", name)
}

type RoundRobin struct {
	backends []*Backend
	next     atomic.Uint64
}

func (p *RoundRobin) Name() string { return "round-robin" }

func (p *RoundRobin) Order(string) []*Backend {
	return rotate(p.backends, int(p.next.Add(1)-1))
}

// LeastLoaded prefers the backend with the fewest requests in flight. Ties go round-robin so an
// idle pool still spreads requests.
type LeastLoaded struct {
	backends []*Backend
	next     atomic.Uint64
}

func (p *LeastLoaded) Name() string { return "least-loaded" }

func (p *LeastLoaded) Order(string) []*Backend {
	out := rotate(p.backends, int(p.next.Add(1)-1))
	sort.SliceStable(out, func(i, j int) bool { return out[i].Inflight() < out[j].Inflight() })
	return out
}

// PrefixAffinity sends requests that share a prefix to the same backend, so its prefix cache
// gets reused. It uses consistent hashing with bounded loads (Mirrokni, Thorup and Zadimoghaddam,
// 2018): a backend whose in-flight count would exceed loadFactor times the average is skipped
// for the next one on the ring, so a hot prefix spills over instead of queueing. A backend that
// is Full is skipped too, because near saturation loadFactor times the average exceeds what a
// backend can run at once. If every backend is full, the request queues at its home backend,
// where its prefix is cached.
type PrefixAffinity struct {
	backends   []*Backend
	loadFactor float64
	ring       []vnode
	fallback   LeastLoaded
}

type vnode struct {
	hash    uint64
	backend int
}

const vnodesPerBackend = 128

func NewPrefixAffinity(backends []*Backend, loadFactor float64) *PrefixAffinity {
	if loadFactor < 1 {
		loadFactor = 1.25
	}
	p := &PrefixAffinity{backends: backends, loadFactor: loadFactor, fallback: LeastLoaded{backends: backends}}
	for i, b := range backends {
		for v := 0; v < vnodesPerBackend; v++ {
			p.ring = append(p.ring, vnode{hash64(fmt.Sprintf("%s#%d", b.Name, v)), i})
		}
	}
	sort.Slice(p.ring, func(i, j int) bool { return p.ring[i].hash < p.ring[j].hash })
	return p
}

func (p *PrefixAffinity) Name() string { return "prefix-affinity" }

func (p *PrefixAffinity) Order(key string) []*Backend {
	if key == "" {
		return p.fallback.Order(key)
	}
	// Walk the ring clockwise from the key, collecting each backend the first time it appears.
	h := hash64(key)
	start := sort.Search(len(p.ring), func(i int) bool { return p.ring[i].hash >= h })
	walk := make([]*Backend, 0, len(p.backends))
	seen := make([]bool, len(p.backends))
	for i := 0; i < len(p.ring) && len(walk) < len(p.backends); i++ {
		v := p.ring[(start+i)%len(p.ring)]
		if !seen[v.backend] {
			seen[v.backend] = true
			walk = append(walk, p.backends[v.backend])
		}
	}
	// The first backend under the load bound goes first; the rest keep ring order for retries.
	var total int64
	for _, b := range p.backends {
		total += b.Inflight()
	}
	limit := int64(math.Ceil(p.loadFactor * float64(total+1) / float64(len(p.backends))))
	for i, b := range walk {
		if b.Inflight()+1 <= limit && !b.Full() {
			return append([]*Backend{b}, append(walk[:i:i], walk[i+1:]...)...)
		}
	}
	return walk
}

func rotate(bs []*Backend, k int) []*Backend {
	out := make([]*Backend, len(bs))
	for i := range bs {
		out[i] = bs[(k+i)%len(bs)]
	}
	return out
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	// FNV-1a mixes short, similar keys poorly; a 64-bit finaliser spreads them over the ring.
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
