// Package topology coordinates the subscription → node pool → platform view pipeline.
// It owns the GlobalNodePool, PlatformManager, and SubscriptionManager,
// breaking import cycles between the leaf packages (node, subscription, platform).
package topology

import (
	"encoding/json"
	"errors"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/puzpuzpuz/xsync/v4"
)

// GlobalNodePool is the system's single source of truth for nodes.
// It uses xsync.Map for concurrent access and xsync.Compute for atomic
// AddNodeFromSub / RemoveNodeFromSub operations.
type GlobalNodePool struct {
	nodes *xsync.Map[node.Hash, *node.NodeEntry]

	// Platform references for dirty-notify.
	platMu         sync.RWMutex
	platformByID   map[string]*platform.Platform // id -> platform
	platformByName map[string]*platform.Platform // name -> platform

	// Subscription lookup — injected by SubscriptionManager.
	subLookup func(subID string) *subscription.Subscription

	// GeoIP lookup — injected at construction.
	geoLookup platform.GeoLookupFunc

	// Persistence callbacks (optional, nil in tests without persistence).
	onNodeAdded      func(hash node.Hash)                        // called after a new node is created
	onNodeRemoved    func(hash node.Hash, entry *node.NodeEntry) // called after a node is deleted from pool
	onSubNodeChanged func(subID string, hash node.Hash, added bool)

	// Health callbacks (optional).
	onNodeDynamicChanged func(hash node.Hash)                // fired on circuit/failure/egress changes
	onNodeLatencyChanged func(hash node.Hash, domain string) // fired on latency upserts and evictions

	// Health config
	maxLatencyTableEntries int
	maxConsecutiveFailures func() int
	latencyDecayWindow     func() time.Duration
	latencyAuthorities     func() []string

	// dirtyQueue coalesces platform re-evaluations. Producers enqueue node
	// hashes without blocking; the drainer applies each hash at most once.
	// When the buffer is full the producer applies inline, so a notification
	// is never dropped — only deferred under sustained backpressure.
	dirtyQueue   chan node.Hash
	dirtyPending map[node.Hash]bool
	dirtyMu      sync.Mutex
	// dirtyQueueFiltered carries hashes that only latency-filtered platforms
	// need to re-evaluate (authority latency samples), so the drainer skips
	// platforms with MaxReferenceLatencyMs == 0.
	dirtyQueueFiltered chan node.Hash
	dirtyStop          chan struct{}
	dirtyStopOnce      sync.Once
	dirtyWG            sync.WaitGroup
	// dirtyApplied is incremented after a notification is fully applied. The
	// flush helper uses it to distinguish "drained" from "never started".
	dirtyApplied atomic.Int64
	// dirtyBusy is 1 while the drainer is inside an apply, 0 when parked in
	// select. FlushPlatformDirty uses it to prove nothing is in flight.
	dirtyBusy atomic.Int32
	// dirtyRunning is set by StartPlatformDirtyWorker. While false, the async
	// helpers apply inline so no notification is dropped.
	dirtyRunning atomic.Bool
}

// PlatformDirtyQueueSize is the coalescing queue capacity for node-hash dirty
// notifications. Overflow is safe: a full queue falls back to mark-and-flush,
// so notifications are delayed but still applied.
const PlatformDirtyQueueSize = 4096

// PoolConfig configures the GlobalNodePool.
type PoolConfig struct {
	SubLookup              func(subID string) *subscription.Subscription
	GeoLookup              platform.GeoLookupFunc
	OnNodeAdded            func(hash node.Hash)
	OnNodeRemoved          func(hash node.Hash, entry *node.NodeEntry)
	OnSubNodeChanged       func(subID string, hash node.Hash, added bool)
	OnNodeDynamicChanged   func(hash node.Hash)
	OnNodeLatencyChanged   func(hash node.Hash, domain string)
	MaxLatencyTableEntries int
	MaxConsecutiveFailures func() int
	LatencyDecayWindow     func() time.Duration
	LatencyAuthorities     func() []string
}

var (
	// ErrPlatformNotRegistered indicates the target platform ID is not registered.
	ErrPlatformNotRegistered = errors.New("platform not registered")
	// ErrPlatformNameConflict indicates another platform already uses the target name.
	ErrPlatformNameConflict = errors.New("platform name conflict")
)

// NewGlobalNodePool creates a new GlobalNodePool.
func NewGlobalNodePool(cfg PoolConfig) *GlobalNodePool {
	maxConsecutiveFailuresFn := cfg.MaxConsecutiveFailures
	if maxConsecutiveFailuresFn == nil {
		panic("topology: NewGlobalNodePool requires non-nil MaxConsecutiveFailures")
	}

	pool := &GlobalNodePool{
		nodes:                  xsync.NewMap[node.Hash, *node.NodeEntry](),
		subLookup:              cfg.SubLookup,
		geoLookup:              cfg.GeoLookup,
		onNodeAdded:            cfg.OnNodeAdded,
		onNodeRemoved:          cfg.OnNodeRemoved,
		onSubNodeChanged:       cfg.OnSubNodeChanged,
		onNodeDynamicChanged:   cfg.OnNodeDynamicChanged,
		onNodeLatencyChanged:   cfg.OnNodeLatencyChanged,
		maxLatencyTableEntries: cfg.MaxLatencyTableEntries,
		maxConsecutiveFailures: maxConsecutiveFailuresFn,
		latencyDecayWindow:     cfg.LatencyDecayWindow,
		latencyAuthorities:     cfg.LatencyAuthorities,
		platformByID:           make(map[string]*platform.Platform),
		platformByName:         make(map[string]*platform.Platform),
		dirtyQueue:             make(chan node.Hash, PlatformDirtyQueueSize),
		dirtyQueueFiltered:     make(chan node.Hash, PlatformDirtyQueueSize),
		dirtyPending:           make(map[node.Hash]bool),
		dirtyStop:              make(chan struct{}),
	}
	return pool
}

// AddNodeFromSub adds a node to the pool with the given subscription reference.
// Uses xsync.Compute for atomic load-or-create + ref-update.
// Idempotent: adding the same (hash, subID) pair multiple times is safe.
// After mutation, notifies all platforms to re-evaluate the node.
func (p *GlobalNodePool) AddNodeFromSub(hash node.Hash, rawOpts json.RawMessage, subID string) {
	isNew := false
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded {
			createdAt := time.Now()
			entry = node.NewNodeEntry(hash, rawOpts, createdAt, p.maxLatencyTableEntries)
			// New subscription nodes start as circuit-open and must be proven healthy by probes.
			entry.CircuitOpenSince.Store(createdAt.UnixNano())
			isNew = true
		}
		entry.AddSubscriptionID(subID)
		return entry, xsync.UpdateOp
	})

	if isNew && p.onNodeAdded != nil {
		p.onNodeAdded(hash)
	}
	if p.onSubNodeChanged != nil {
		p.onSubNodeChanged(subID, hash, true)
	}

	p.notifyAllPlatformsDirty(hash)
}

// RemoveNodeFromSub removes a subscription reference from a node.
// If the node has no remaining references, it is deleted from the pool.
// Uses xsync.Compute for atomic ref-update + conditional delete.
// Idempotent: removing a nonexistent (hash, subID) pair is safe.
func (p *GlobalNodePool) RemoveNodeFromSub(hash node.Hash, subID string) {
	wasDeleted := false
	var deletedEntry *node.NodeEntry // capture entry before map deletion
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded {
			return entry, xsync.CancelOp // idempotent no-op
		}
		empty := entry.RemoveSubscriptionID(subID)
		if empty {
			wasDeleted = true
			deletedEntry = entry
			return nil, xsync.DeleteOp
		}
		return entry, xsync.UpdateOp
	})

	if p.onSubNodeChanged != nil {
		p.onSubNodeChanged(subID, hash, false)
	}
	if wasDeleted && p.onNodeRemoved != nil {
		p.onNodeRemoved(hash, deletedEntry)
	}

	p.notifyAllPlatformsDirty(hash)
}

// GetEntry retrieves a node entry by hash.
func (p *GlobalNodePool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	return p.nodes.Load(hash)
}

// Range iterates all nodes in the pool.
func (p *GlobalNodePool) Range(fn func(node.Hash, *node.NodeEntry) bool) {
	p.nodes.Range(fn)
}

// Size returns the number of nodes in the pool.
func (p *GlobalNodePool) Size() int {
	return p.nodes.Size()
}

// LoadNodeFromBootstrap inserts a node during bootstrap recovery.
// No dirty-marks, no platform notifications.
func (p *GlobalNodePool) LoadNodeFromBootstrap(entry *node.NodeEntry) {
	p.nodes.Store(entry.Hash, entry)
}

// RegisterPlatform adds a platform to receive dirty notifications.
func (p *GlobalNodePool) RegisterPlatform(plat *platform.Platform) {
	p.platMu.Lock()
	defer p.platMu.Unlock()
	// Check for existing ID to avoid duplicates.
	if _, exists := p.platformByID[plat.ID]; exists {
		return
	}
	p.platformByID[plat.ID] = plat
	if plat.Name != "" {
		p.platformByName[plat.Name] = plat
	}
}

// UnregisterPlatform removes a platform from dirty notifications.
func (p *GlobalNodePool) UnregisterPlatform(id string) {
	p.platMu.Lock()
	defer p.platMu.Unlock()
	plat, ok := p.platformByID[id]
	if !ok {
		return
	}
	delete(p.platformByID, id)
	if plat.Name != "" {
		// Only delete when name still points at this platform.
		if current, ok := p.platformByName[plat.Name]; ok && current == plat {
			delete(p.platformByName, plat.Name)
		}
	}
}

// ReplacePlatform atomically replaces an existing platform object by ID.
// It follows a copy-on-write update path: the caller builds a new Platform
// instance, this method rebuilds its routable view, then swaps map pointers
// under platMu in one critical section.
func (p *GlobalNodePool) ReplacePlatform(next *platform.Platform) error {
	if next == nil || next.ID == "" {
		return ErrPlatformNotRegistered
	}

	// Build the new platform's view before publish so readers never observe
	// an empty, not-yet-built view due only to replacement.
	p.RebuildPlatform(next)

	p.platMu.Lock()
	defer p.platMu.Unlock()

	current, ok := p.platformByID[next.ID]
	if !ok {
		return ErrPlatformNotRegistered
	}

	if next.Name != "" {
		if existingByName, exists := p.platformByName[next.Name]; exists && existingByName != current {
			return ErrPlatformNameConflict
		}
	}

	p.platformByID[next.ID] = next

	if current.Name != "" {
		if mapped, exists := p.platformByName[current.Name]; exists && mapped == current {
			delete(p.platformByName, current.Name)
		}
	}
	if next.Name != "" {
		p.platformByName[next.Name] = next
	}

	return nil
}

// GetPlatform retrieves a platform by ID.
func (p *GlobalNodePool) GetPlatform(id string) (*platform.Platform, bool) {
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByID[id]
	return plat, ok
}

// GetPlatformByName retrieves a platform by Name.
func (p *GlobalNodePool) GetPlatformByName(name string) (*platform.Platform, bool) {
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByName[name]
	return plat, ok
}

// RangePlatforms iterates all registered platforms.
func (p *GlobalNodePool) RangePlatforms(fn func(*platform.Platform) bool) {
	for _, plat := range p.platformSnapshot() {
		if !fn(plat) {
			return
		}
	}
}

func (p *GlobalNodePool) platformSnapshot() []*platform.Platform {
	p.platMu.RLock()
	defer p.platMu.RUnlock()

	platforms := make([]*platform.Platform, 0, len(p.platformByID))
	for _, plat := range p.platformByID {
		platforms = append(platforms, plat)
	}
	return platforms
}

// MakeSubLookup builds the SubLookupFunc closure for MatchRegexs / tag resolution.
func (p *GlobalNodePool) MakeSubLookup() node.SubLookupFunc {
	return func(subID string, hash node.Hash) (string, bool, []string, bool) {
		// Compatibility fallback for test wiring that omits SubLookup.
		// We cannot resolve subscription metadata, so treat the reference as
		// "present+enabled" without tags.
		if p.subLookup == nil {
			return "", true, nil, true
		}

		sub := p.subLookup(subID)
		if sub == nil {
			return "", false, nil, false
		}

		managed, ok := sub.ManagedNodes().LoadNode(hash)
		if !ok || managed.Evicted {
			return "", false, nil, false
		}
		tags := managed.Tags
		return sub.Name(), sub.Enabled(), tags, true
	}
}

// ResolveNodeDisplayTag resolves a node hash to its display tag for request logs.
// Rule:
//  1. Prefer enabled subscriptions: among enabled holders, choose earliest-created.
//  2. Within that subscription, choose lexicographically smallest tag.
//  3. If no enabled holder exists, fallback to all holders with the same rule.
//  4. Return "<SubscriptionName>/<Tag>".
//
// Returns empty string when resolution is not possible.
func (p *GlobalNodePool) ResolveNodeDisplayTag(hash node.Hash) string {
	if p.subLookup == nil {
		return ""
	}

	entry, ok := p.GetEntry(hash)
	if !ok || entry == nil {
		return ""
	}
	subIDs := entry.SubscriptionIDs()
	if len(subIDs) == 0 {
		return ""
	}

	pick := func(enabledOnly bool) (string, bool) {
		bestFound := false
		var bestCreatedAtNs int64
		var bestSubID string
		var bestSubName string
		var bestTag string

		for _, subID := range subIDs {
			sub := p.subLookup(subID)
			if sub == nil {
				continue
			}
			if enabledOnly && !sub.Enabled() {
				continue
			}

			managed, ok := sub.ManagedNodes().LoadNode(hash)
			if !ok || managed.Evicted {
				continue
			}
			tags := managed.Tags
			if len(tags) == 0 {
				continue
			}

			smallestTag := tags[0]
			for _, tag := range tags[1:] {
				if tag < smallestTag {
					smallestTag = tag
				}
			}

			createdAtNs := sub.CreatedAtNs
			if !bestFound ||
				createdAtNs < bestCreatedAtNs ||
				(createdAtNs == bestCreatedAtNs && subID < bestSubID) {
				bestFound = true
				bestCreatedAtNs = createdAtNs
				bestSubID = subID
				bestSubName = sub.Name()
				bestTag = smallestTag
			}
		}

		if !bestFound || bestSubName == "" || bestTag == "" {
			return "", false
		}
		return bestSubName + "/" + bestTag, true
	}

	if tag, ok := pick(true); ok {
		return tag
	}
	if tag, ok := pick(false); ok {
		return tag
	}
	return ""
}

// IsNodeDisabled reports whether a node is disabled by subscription state:
// all referencing subscriptions are disabled (or missing / not applicable).
func (p *GlobalNodePool) IsNodeDisabled(hash node.Hash) bool {
	entry, ok := p.GetEntry(hash)
	if !ok || entry == nil {
		return true
	}
	return entry.IsDisabledBySubscriptions(p.MakeSubLookup())
}

// MakeHealthyAndEnabledEvaluator builds a predicate for pool-context health
// aggregates: the node must not be disabled by subscription state and must
// satisfy the entry-local health checks.
func (p *GlobalNodePool) MakeHealthyAndEnabledEvaluator() func(entry *node.NodeEntry) bool {
	subLookup := p.MakeSubLookup()
	return func(entry *node.NodeEntry) bool {
		if entry == nil || entry.IsDisabledBySubscriptions(subLookup) {
			return false
		}
		return entry.IsHealthy()
	}
}

// notifyAllPlatformsDirty tells every registered platform to re-evaluate a node.
//
// This is the synchronous variant: it applies the re-evaluation before
// returning, and it "consumes" a queued notification for the same hash so the
// drainer does not apply the same node twice. Production hot paths use
// markNodeDirtyAsync instead; tests and control-plane operations that assert
// view contents immediately after a mutation use this one.
func (p *GlobalNodePool) notifyAllPlatformsDirty(hash node.Hash) {
	if len(p.platformSnapshot()) == 0 {
		return
	}

	p.dirtyMu.Lock()
	if p.dirtyPending[hash] {
		p.dirtyPending[hash] = false // suppress coalescing for this hash
		p.dirtyMu.Unlock()
		p.applyPlatformDirty(hash)
		return
	}
	p.dirtyPending[hash] = false
	p.dirtyMu.Unlock()

	p.applyPlatformDirty(hash)
}

// markNodeDirtyAsync is the non-blocking variant used on hot paths (probe
// workers, passive health feedback). It enqueues the hash for the drainer and
// returns immediately. If the buffer is full it falls back to an inline apply,
// so a notification is never dropped.
func (p *GlobalNodePool) markNodeDirtyAsync(hash node.Hash) {
	if !p.dirtyRunning.Load() {
		// No drainer yet (tests, or pre-start): apply inline so the
		// notification is never silently dropped.
		p.applyPlatformDirty(hash)
		return
	}

	p.dirtyMu.Lock()
	if pending, ok := p.dirtyPending[hash]; ok && pending {
		p.dirtyMu.Unlock()
		return // already queued; one apply covers both
	}
	p.dirtyPending[hash] = true
	p.dirtyMu.Unlock()

	select {
	case p.dirtyQueue <- hash:
		return
	default:
	}

	// Backpressure: apply inline so nothing is lost.
	p.dirtyMu.Lock()
	delete(p.dirtyPending, hash)
	p.dirtyMu.Unlock()
	p.applyPlatformDirty(hash)
}

// markNodeFilteredDirtyAsync is the latency-filtered counterpart of
// markNodeDirtyAsync: it queues the hash for the drainer and returns
// immediately. The drainer applies it only to platforms whose routable view
// depends on reference latency (MaxReferenceLatencyMs > 0), preserving the
// v1.2.3 optimisation that avoids re-evaluating latency-agnostic platforms on
// every authority latency sample.
func (p *GlobalNodePool) markNodeFilteredDirtyAsync(hash node.Hash) {
	if !p.dirtyRunning.Load() {
		p.applyFilteredDirty(hash)
		return
	}
	select {
	case p.dirtyQueueFiltered <- hash:
	case <-p.dirtyStop:
	}
}

// StartPlatformDirtyWorker launches the drainer. Must be called once, before
// health/latency feedback flows, and paired with ClosePlatformDirtyWorker.
func (p *GlobalNodePool) StartPlatformDirtyWorker() {
	p.dirtyRunning.Store(true)
	p.dirtyWG.Add(1)
	go func() {
		defer p.dirtyWG.Done()
		p.drainPlatformDirty()
	}()
}

// ClosePlatformDirtyWorker stops the drainer and flushes anything still queued.
func (p *GlobalNodePool) ClosePlatformDirtyWorker() {
	p.dirtyStopOnce.Do(func() { close(p.dirtyStop) })
	p.dirtyWG.Wait()
}

// FlushPlatformDirty applies every pending dirty notification and returns once
// the platform views reflect them.
//
// This exists for callers that must observe view contents deterministically
// right after a mutation (tests, control-plane paths that immediately read a
// platform's routable set). Production request/probe paths never call it —
// they accept the drainer's eventual consistency.
func (p *GlobalNodePool) FlushPlatformDirty() {
	if !p.dirtyRunning.Load() {
		return // no drainer: helpers applied inline, nothing to wait for
	}
	deadline := time.Now().Add(flushWaitTimeout)
	emptyStreak := 0
	for time.Now().Before(deadline) {
		p.dirtyMu.Lock()
		pending := len(p.dirtyPending)
		p.dirtyMu.Unlock()
		queued := p.dirtyQueued()
		inFlight := p.dirtyBusy.Load() != 0

		if pending == 0 && queued == 0 && !inFlight {
			emptyStreak++
			// Two consecutive calm samples (a drainer that just picked work up
			// will show busy on at least one of them) means nothing is left.
			if emptyStreak >= 2 {
				return
			}
		} else {
			emptyStreak = 0
		}
		time.Sleep(flushPollInterval)
	}
}

// dirtyQueued reports how many notifications are still waiting to be applied
// (in flight or buffered). It is read without holding dirtyMu so the flush
// loop never blocks the drainer it is waiting for.
func (p *GlobalNodePool) dirtyQueued() int {
	return len(p.dirtyQueue) + len(p.dirtyQueueFiltered)
}

const (
	flushWaitTimeout  = 2 * time.Second
	flushPollInterval = time.Millisecond
)

func (p *GlobalNodePool) drainPlatformDirty() {
	for {
		// Parked in select: nothing in flight.
		p.dirtyBusy.Store(0)
		select {
		case hash := <-p.dirtyQueue:
			p.dirtyBusy.Store(1)
			p.applyDirty(hash)
		case hash := <-p.dirtyQueueFiltered:
			p.dirtyBusy.Store(1)
			p.applyFilteredDirty(hash)
		case <-p.dirtyStop:
			// Flush what is still queued so shutdown loses no re-evaluation.
		drainLoop:
			for {
				select {
				case hash := <-p.dirtyQueue:
					p.dirtyBusy.Store(1)
					p.applyDirty(hash)
				case hash := <-p.dirtyQueueFiltered:
					p.dirtyBusy.Store(1)
					p.applyFilteredDirty(hash)
				default:
					break drainLoop
				}
			}
			p.dirtyBusy.Store(0)
			return
		}
	}
}

// applyDirty clears the coalescing mark for hash and runs the re-evaluation
// across all platforms.
func (p *GlobalNodePool) applyDirty(hash node.Hash) {
	p.dirtyMu.Lock()
	delete(p.dirtyPending, hash)
	p.dirtyMu.Unlock()
	p.applyPlatformDirty(hash)
	p.dirtyApplied.Add(1)
}

// applyFilteredDirty re-evaluates hash on latency-filtered platforms only.
func (p *GlobalNodePool) applyFilteredDirty(hash node.Hash) {
	platforms := p.platformSnapshot()
	filtered := make([]*platform.Platform, 0, len(platforms))
	for _, plat := range platforms {
		if plat.MaxReferenceLatencyMs > 0 {
			filtered = append(filtered, plat)
		}
	}
	if len(filtered) == 0 {
		return
	}

	subLookup := p.MakeSubLookup()
	getEntry := func(h node.Hash) (*node.NodeEntry, bool) {
		return p.nodes.Load(h)
	}
	for _, plat := range filtered {
		plat.NotifyDirty(hash, getEntry, subLookup, p.geoLookup, p.latencyAuthorities)
	}
	p.dirtyApplied.Add(1)
}

// applyPlatformDirty runs the actual per-platform re-evaluation for one node.
// This is the body the previous implementation executed synchronously in every
// caller; it is unchanged apart from being reached via the queue.
func (p *GlobalNodePool) applyPlatformDirty(hash node.Hash) {
	platforms := p.platformSnapshot()
	if len(platforms) == 0 {
		return
	}

	subLookup := p.MakeSubLookup()
	getEntry := func(h node.Hash) (*node.NodeEntry, bool) {
		return p.nodes.Load(h)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(platforms) {
		workers = len(platforms)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, plat := range platforms {
		sem <- struct{}{}
		wg.Add(1)
		go func(plat *platform.Platform) {
			defer wg.Done()
			defer func() { <-sem }()
			plat.NotifyDirty(hash, getEntry, subLookup, p.geoLookup, p.latencyAuthorities)
		}(plat)
	}
	wg.Wait()
}

// RebuildAllPlatforms triggers a full rebuild on all registered platforms.
func (p *GlobalNodePool) RebuildAllPlatforms() {
	platforms := p.platformSnapshot()
	if len(platforms) == 0 {
		return
	}

	subLookup := p.MakeSubLookup()
	poolRange := func(fn func(node.Hash, *node.NodeEntry) bool) {
		p.nodes.Range(fn)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(platforms) {
		workers = len(platforms)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, plat := range platforms {
		sem <- struct{}{}
		wg.Add(1)
		go func(plat *platform.Platform) {
			defer wg.Done()
			defer func() { <-sem }()
			plat.FullRebuild(poolRange, subLookup, p.geoLookup, p.latencyAuthorities)
		}(plat)
	}
	wg.Wait()
}

// RebuildPlatform triggers a full rebuild on a specific platform.
func (p *GlobalNodePool) RebuildPlatform(plat *platform.Platform) {
	subLookup := p.MakeSubLookup()
	poolRange := func(fn func(node.Hash, *node.NodeEntry) bool) {
		p.nodes.Range(fn)
	}
	plat.FullRebuild(poolRange, subLookup, p.geoLookup, p.latencyAuthorities)
}

// --- Health Management ---

// SetOnNodeAdded sets the callback fired when a new node is added.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeAdded(fn func(hash node.Hash)) {
	p.onNodeAdded = fn
}

// SetOnNodeRemoved sets the callback fired when a node is removed from the pool.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeRemoved(fn func(hash node.Hash, entry *node.NodeEntry)) {
	p.onNodeRemoved = fn
}

// NotifyNodeDirty triggers platform re-evaluation for a single node.
// Used by OutboundManager after outbound creation to update routable views.
func (p *GlobalNodePool) NotifyNodeDirty(hash node.Hash) {
	p.notifyAllPlatformsDirty(hash)
}

// RangeNodes iterates over all nodes in the pool.
// The callback receives each node's hash and entry. Return false to stop.
func (p *GlobalNodePool) RangeNodes(fn func(node.Hash, *node.NodeEntry) bool) {
	p.nodes.Range(fn)
}

// RecordResult records a probe or passive health-check result.
// On success, resets FailureCount and clears circuit-breaker.
// On failure, increments FailureCount and opens circuit-breaker if threshold is reached.
// Notifies platforms only when circuit state changes (open/recover).
// Fires OnNodeDynamicChanged only when dynamic fields actually change.
func (p *GlobalNodePool) RecordResult(hash node.Hash, success bool) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	dynamicChanged := false
	circuitStateChanged := false

	if success {
		if entry.FailureCount.Swap(0) != 0 {
			dynamicChanged = true
		}
		if entry.CircuitOpenSince.Swap(0) != 0 {
			dynamicChanged = true
			circuitStateChanged = true
		}
	} else {
		newCount := entry.FailureCount.Add(1)
		dynamicChanged = true
		maxConsecutiveFailures := p.currentMaxConsecutiveFailures()
		if maxConsecutiveFailures > 0 && int(newCount) >= maxConsecutiveFailures {
			// Open circuit if not already open.
			if entry.CircuitOpenSince.CompareAndSwap(0, time.Now().UnixNano()) {
				circuitStateChanged = true
			}
		}
	}

	// Hot path: probe workers and passive request-path feedback must not block
	// on platform view locks. Coalesced + drained in the background.
	// The dirty mark is only needed when circuit state actually changed —
	// otherwise every successful probe result re-enqueues the node.
	if circuitStateChanged {
		p.markNodeDirtyAsync(hash)
	}
	if dynamicChanged && p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
}

// RecordPassiveResult records health feedback from user proxy traffic.
// Failed passive traffic is ignored when the originating platform disables
// passive circuit breaking; successes still count as positive health feedback.
func (p *GlobalNodePool) RecordPassiveResult(platformID string, hash node.Hash, success bool) {
	if success || !p.passiveCircuitBreakerDisabled(platformID) {
		p.RecordResult(hash, success)
	}
}

func (p *GlobalNodePool) passiveCircuitBreakerDisabled(platformID string) bool {
	if platformID == "" {
		return false
	}

	p.platMu.RLock()
	defer p.platMu.RUnlock()

	plat, ok := p.platformByID[platformID]
	if !ok || plat == nil {
		return false
	}
	return plat.PassiveCircuitBreakerDisabled
}

func (p *GlobalNodePool) currentMaxConsecutiveFailures() int {
	return p.maxConsecutiveFailures()
}

// RecordLatency records a latency probe attempt for the given node and raw target.
// rawTarget is normalized through ExtractDomain (eTLD+1). latency may be nil,
// which means "attempt only" without latency sample writeback.
func (p *GlobalNodePool) RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	domain := netutil.ExtractDomain(rawTarget)
	isAuthority := p.isAuthorityDomain(domain)
	nowNs := time.Now().UnixNano()
	entry.LastLatencyProbeAttempt.Store(nowNs)
	if isAuthority {
		entry.LastAuthorityLatencyProbeAttempt.Store(nowNs)
	}
	if p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}

	if latency == nil || *latency <= 0 || entry.LatencyTable == nil {
		return
	}

	var decayWindow time.Duration
	if p.latencyDecayWindow != nil {
		decayWindow = p.latencyDecayWindow()
	}
	if decayWindow <= 0 {
		decayWindow = 30 * time.Second // default
	}

	wasEmpty, evictedDomain, evicted := entry.LatencyTable.UpdateClassified(domain, *latency, decayWindow, isAuthority)

	// A latency-filtered platform must be re-evaluated when an authority sample
	// changes. Platforms without a reference-latency limit do not depend on it.
	// Both branches are hot paths (every probe writes latency), so they enqueue
	// rather than blocking on platform view locks — but the "filtered only"
	// optimisation from v1.2.3 is preserved.
	if wasEmpty {
		p.markNodeDirtyAsync(hash)
	} else if isAuthority {
		p.markNodeFilteredDirtyAsync(hash)
	}

	if p.onNodeLatencyChanged != nil {
		p.onNodeLatencyChanged(hash, domain)
		if evicted {
			p.onNodeLatencyChanged(hash, evictedDomain)
		}
	}
}

// notifyLatencyFilteredPlatformsDirty re-evaluates only platforms whose
// routable view depends on reference latency samples.
func (p *GlobalNodePool) notifyLatencyFilteredPlatformsDirty(hash node.Hash) {
	platforms := p.platformSnapshot()
	filtered := make([]*platform.Platform, 0, len(platforms))
	for _, plat := range platforms {
		if plat.MaxReferenceLatencyMs > 0 {
			filtered = append(filtered, plat)
		}
	}
	if len(filtered) == 0 {
		return
	}

	subLookup := p.MakeSubLookup()
	getEntry := func(h node.Hash) (*node.NodeEntry, bool) {
		return p.nodes.Load(h)
	}
	for _, plat := range filtered {
		plat.NotifyDirty(hash, getEntry, subLookup, p.geoLookup, p.latencyAuthorities)
	}
}

// UpdateNodeEgressIP records an egress probe attempt and optionally updates
// the node's egress IP and explicit region metadata.
// Region update rules:
//   - ip=nil,  loc=nil: keep both IP and region unchanged.
//   - ip!=nil, loc=nil: keep region if IP unchanged; clear region if IP changed.
//   - loc!=nil: set region to loc (normalized).
func (p *GlobalNodePool) UpdateNodeEgressIP(hash node.Hash, ip *netip.Addr, loc *string) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	nowNs := time.Now().UnixNano()
	entry.LastEgressUpdateAttempt.Store(nowNs)

	oldIP := entry.GetEgressIP()
	oldRegion := entry.GetEgressRegion()
	ipChanged := false

	if ip != nil {
		// Record successful egress-IP sample timestamp.
		entry.LastEgressUpdate.Store(nowNs)
		if oldIP != *ip {
			entry.SetEgressIP(*ip)
			ipChanged = true
		}
	}

	regionChanged := false
	switch {
	case loc != nil:
		entry.SetEgressRegion(*loc)
		regionChanged = oldRegion != entry.GetEgressRegion()
	case ip == nil:
		// Attempt-only update: keep region as-is.
	case !ipChanged:
		// IP unchanged and no explicit region: keep existing region.
	default:
		// IP changed without explicit region: clear stale region metadata.
		if oldRegion != "" {
			entry.SetEgressRegion("")
			regionChanged = true
		}
	}

	if ipChanged || regionChanged {
		// Hot path: an egress probe changing region/IP can arrive for hundreds
		// of nodes in one scan. Enqueue instead of blocking on view locks.
		p.markNodeDirtyAsync(hash)
	}
	if p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
}

func (p *GlobalNodePool) isAuthorityDomain(domain string) bool {
	if domain == "" || p.latencyAuthorities == nil {
		return false
	}
	authorities := p.latencyAuthorities()
	for _, authority := range authorities {
		if strings.EqualFold(strings.TrimSpace(authority), domain) {
			return true
		}
	}
	return false
}
