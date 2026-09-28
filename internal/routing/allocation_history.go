package routing

import (
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/puzpuzpuz/xsync/v4"
)

const nodeAllocationCooldown = time.Hour

// NodeAllocationHistory tracks each node's latest assignment time per platform.
type NodeAllocationHistory struct {
	lastAllocated *xsync.Map[node.Hash, int64]
	writes        atomic.Uint64
}

// NewNodeAllocationHistory creates an empty history tracker.
func NewNodeAllocationHistory() *NodeAllocationHistory {
	return &NodeAllocationHistory{
		lastAllocated: xsync.NewMap[node.Hash, int64](),
	}
}

// Record advances the last assignment time for a node.
func (h *NodeAllocationHistory) Record(hash node.Hash, allocatedAtNs int64) {
	if h == nil || hash == node.Zero || allocatedAtNs <= 0 {
		return
	}
	h.lastAllocated.Compute(hash, func(previous int64, loaded bool) (int64, xsync.ComputeOp) {
		if !loaded || allocatedAtNs > previous {
			return allocatedAtNs, xsync.UpdateOp
		}
		return previous, xsync.CancelOp
	})

	if h.writes.Add(1)%256 == 0 {
		cutoff := allocatedAtNs - int64(nodeAllocationCooldown)
		h.lastAllocated.DeleteMatching(func(_ node.Hash, lastAllocatedNs int64) (bool, bool) {
			return lastAllocatedNs <= cutoff, false
		})
	}
}

// WasRecentlyAllocated reports whether the node was assigned within the last hour.
func (h *NodeAllocationHistory) WasRecentlyAllocated(hash node.Hash, nowNs int64) bool {
	if h == nil {
		return false
	}
	allocatedAtNs, ok := h.lastAllocated.Load(hash)
	return ok && nowNs-allocatedAtNs < int64(nodeAllocationCooldown)
}
