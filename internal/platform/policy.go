package platform

// AllocationPolicy controls how routing scores candidates for new leases.
type AllocationPolicy string

const (
	AllocationPolicyBalanced                AllocationPolicy = "BALANCED"
	AllocationPolicyPreferLowLatency        AllocationPolicy = "PREFER_LOW_LATENCY"
	AllocationPolicyPreferIdleIP            AllocationPolicy = "PREFER_IDLE_IP"
	AllocationPolicyPreferLowLatencyAndIdle AllocationPolicy = "PREFER_LOW_LATENCY_AND_IDLE"
)

// ParseAllocationPolicy normalizes external string input into a supported policy.
// Unknown values fall back to BALANCED for compatibility.
func ParseAllocationPolicy(raw string) AllocationPolicy {
	p := AllocationPolicy(raw)
	if p.IsValid() {
		return p
	}
	return AllocationPolicyBalanced
}

func (p AllocationPolicy) IsValid() bool {
	switch p {
	case AllocationPolicyBalanced, AllocationPolicyPreferLowLatency, AllocationPolicyPreferIdleIP,
		AllocationPolicyPreferLowLatencyAndIdle:
		return true
	default:
		return false
	}
}
