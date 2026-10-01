// Package partition implements the shard-ownership model for a multi-replica
// collector fleet: every collector owns the offsets whose residue mod the total
// replica count equals its index (offset % total == index). The state is fed by
// the MQ partition registry, so unique indices need no pod-name surgery and
// scale events rebalance automatically (same machinery as the streamer fleet).
package partition

// State is a single (index, total) assignment.
type State struct {
	Index int
	Total int
}

// Owns reports whether this collector is responsible for the given log offset.
// Residue classes are disjoint for a fixed total, so no two replicas ever write
// the same batch; across a rebalance a previous owner may have already written
// an offset the new owner re-reads, but ClickHouse dedup collapses that.
func (s State) Owns(offset int64) bool {
	if s.Total <= 0 || s.Index < 0 || offset < 0 {
		return false
	}
	return offset%int64(s.Total) == int64(s.Index)
}
