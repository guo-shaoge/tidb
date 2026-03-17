// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package joinorder

import (
	"math/bits"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"go.uber.org/zap"
)

type joinOrderDP struct {
	JoinOrder
}

func newJoinOrderDP(ctx base.PlanContext, group *joinGroup) *joinOrderDP {
	return &joinOrderDP{
		JoinOrder: JoinOrder{
			ctx:   ctx,
			group: group,
		},
	}
}

func (j *joinOrderDP) optimize() (base.LogicalPlan, error) {
	group := j.group
	detector := newConflictDetector(j.ctx)
	nodes, err := detector.Build(group)
	if err != nil {
		return nil, err
	}

	// Handle leading hints (shared with greedy).
	nodeWithHint, nodes, err := j.buildJoinByHint(detector, nodes)
	if err != nil {
		return nil, err
	}
	if len(nodes) < 1 {
		return nodeWithHint.p, nil
	}

	// If there is a hint node, treat it as a "super-vertex" and prepend it.
	if nodeWithHint != nil {
		nodes = append([]*Node{nodeWithHint}, nodes...)
	}

	// Build vertex-to-DP-bit mapping. Each node gets a unique bit position
	// in a uint64 bitmask for efficient subset enumeration.
	vertexToDPBit := make(map[int]int, len(nodes)*2)
	for i, node := range nodes {
		node.bitSet.ForEach(func(v int) {
			vertexToDPBit[v] = i
		})
	}

	// Precompute edge TES masks (in DP bit-space) for the completeness check.
	edgeInfos := buildDPEdgeInfos(detector, vertexToDPBit)

	// Find connected components via BFS over edges.
	components := findConnectedComponents(len(nodes), edgeInfos)

	// Run DPSube on each connected component independently.
	var resultNodes []*Node
	for _, compMask := range components {
		best, err := dpSube(detector, compMask, nodes, edgeInfos, j.group.vertexHints)
		if err != nil {
			return nil, err
		}
		if best == nil {
			// No valid DP result for this component.
			// This can happen if conflict rules prevent all orderings.
			// Collect individual nodes from this component for bushy tree fallback.
			for i := 0; i < len(nodes); i++ {
				if compMask&(1<<uint(i)) != 0 {
					resultNodes = append(resultNodes, nodes[i])
				}
			}
			continue
		}
		resultNodes = append(resultNodes, best)
	}

	// Verify completeness: all edges must be consumed.
	usedEdges := collectUsedEdges(resultNodes)
	if detector.HasRemainingEdges(usedEdges) {
		totalEdges, usedEdgeCount, missingEdges, missingDetail, nodeSets := summarizeEdges(detector, usedEdges, resultNodes, 4)
		logutil.BgLogger().Warn("join reorder DP skipped because not all edges are used",
			zap.Int("rootID", group.root.ID()),
			zap.Int("nodes", len(resultNodes)),
			zap.Int("totalEdges", totalEdges),
			zap.Int("usedEdges", usedEdgeCount),
			zap.Int("missingEdges", missingEdges),
			zap.String("missingDetail", missingDetail),
			zap.String("nodeSets", nodeSets),
			zap.Bool("allInnerJoin", group.allInnerJoin))
		return group.root, nil
	}

	if len(resultNodes) <= 0 {
		return nil, errors.New("internal error: DP result nodes empty")
	}
	return makeBushyTree(j.ctx, resultNodes, j.group.vertexHints)
}

// dpEdgeInfo stores a precomputed DP bitmask for an edge's TES, avoiding
// repeated FastIntSet-to-uint64 conversions in the DP inner loop.
type dpEdgeInfo struct {
	edge    *edge
	tesMask uint64
}

func buildDPEdgeInfos(detector *ConflictDetector, vertexToDPBit map[int]int) []dpEdgeInfo {
	var infos []dpEdgeInfo
	detector.iterateEdges(func(e *edge) bool {
		if len(e.eqConds) == 0 && len(e.nonEQConds) == 0 {
			return true
		}
		infos = append(infos, dpEdgeInfo{
			edge:    e,
			tesMask: fastIntSetToDPMask(e.tes, vertexToDPBit),
		})
		return true
	})
	return infos
}

// fastIntSetToDPMask converts a FastIntSet (using original vertex indices)
// to a uint64 bitmask using DP bit positions.
func fastIntSetToDPMask(s intset.FastIntSet, vertexToDPBit map[int]int) uint64 {
	var mask uint64
	s.ForEach(func(v int) {
		if dpIdx, ok := vertexToDPBit[v]; ok {
			mask |= 1 << uint(dpIdx)
		}
	})
	return mask
}

// findConnectedComponents identifies connected components among the DP nodes
// by BFS over edges. Two nodes are in the same component if there exists any
// edge whose TES touches both.
func findConnectedComponents(n int, edgeInfos []dpEdgeInfo) []uint64 {
	allMask := uint64((1 << uint(n)) - 1)

	// Build adjacency: for each edge, mark which DP nodes it connects.
	adj := make([]uint64, n)
	for _, ei := range edgeInfos {
		mask := ei.tesMask
		for tmp := mask; tmp != 0; {
			i := bits.TrailingZeros64(tmp)
			adj[i] |= mask
			tmp &= tmp - 1
		}
	}

	var components []uint64
	visited := uint64(0)
	for visited != allMask {
		start := bits.TrailingZeros64(allMask &^ visited)
		component := uint64(1 << uint(start))
		queue := component
		for queue != 0 {
			cur := bits.TrailingZeros64(queue)
			queue &= queue - 1
			reachable := adj[cur] &^ component
			component |= reachable
			queue |= reachable
		}
		visited |= component
		components = append(components, component)
	}
	return components
}

// dpSube implements the DPSube algorithm (Dynamic Programming via Subset
// Enumeration). It enumerates all subsets of the given component, and for each
// subset tries all binary partitions to find the best join plan.
func dpSube(
	detector *ConflictDetector,
	componentMask uint64,
	nodes []*Node,
	edgeInfos []dpEdgeInfo,
	vertexHints map[int]*JoinMethodHint,
) (*Node, error) {
	bestPlan := make(map[uint64]*Node)

	// Initialize single-node plans.
	for i, node := range nodes {
		bit := uint64(1 << uint(i))
		if bit&componentMask != 0 {
			bestPlan[bit] = node
		}
	}

	// Enumerate subsets of componentMask from small to large.
	for subset := uint64(1); subset <= componentMask; subset++ {
		if subset&componentMask != subset {
			continue
		}
		if bits.OnesCount64(subset) < 2 {
			continue
		}
		if bestPlan[subset] != nil {
			// Already initialized (e.g. a leading-hint super-vertex).
			continue
		}

		// Enumerate all binary partitions: s1 | s2 = subset, s1 & s2 = 0.
		for s1 := (subset - 1) & subset; s1 > 0; s1 = (s1 - 1) & subset {
			s2 := subset ^ s1
			if s1 > s2 {
				continue // avoid duplicate partitions
			}
			left := bestPlan[s1]
			right := bestPlan[s2]
			if left == nil || right == nil {
				continue
			}

			// Try both directions: (left, right) and (right, left).
			// For non-inner joins, the left/right order matters because
			// checkNonInnerEdgeApplicable enforces side semantics.
			for _, pair := range [2][2]*Node{{left, right}, {right, left}} {
				newNode, err := dpTryJoin(detector, pair[0], pair[1], subset, edgeInfos, vertexHints)
				if err != nil {
					return nil, err
				}
				if newNode == nil {
					continue
				}
				if bestPlan[subset] == nil || newNode.cumCost < bestPlan[subset].cumCost {
					bestPlan[subset] = newNode
				}
			}
		}
	}

	return bestPlan[componentMask], nil
}

// dpTryJoin attempts to join two DP nodes and verifies edge completeness.
func dpTryJoin(
	detector *ConflictDetector,
	left, right *Node,
	subsetMask uint64,
	edgeInfos []dpEdgeInfo,
	vertexHints map[int]*JoinMethodHint,
) (*Node, error) {
	checkResult, err := detector.CheckConnection(left, right)
	if err != nil {
		return nil, err
	}
	if !checkResult.Connected() {
		return nil, nil
	}

	newNode, err := detector.MakeJoin(checkResult, vertexHints)
	if err != nil {
		return nil, err
	}
	if newNode == nil {
		return nil, nil
	}

	// Edge completeness check: all edges whose TES ⊆ subset must have been
	// consumed. This is critical because inner join predicates are split into
	// separate edges (one per conjunct). Without this check, we could produce
	// plans that "forget" some predicates.
	if !checkDPEdgeCompleteness(newNode.usedEdges, subsetMask, edgeInfos) {
		return nil, nil
	}

	return newNode, nil
}

// checkDPEdgeCompleteness verifies that all edges whose TES fits within the
// given subset have been consumed by the node. This is the equivalent of
// CRDB's checkAppliedEdges.
func checkDPEdgeCompleteness(usedEdges map[uint64]struct{}, subsetMask uint64, edgeInfos []dpEdgeInfo) bool {
	for _, ei := range edgeInfos {
		if ei.tesMask != 0 && ei.tesMask&subsetMask == ei.tesMask {
			if _, ok := usedEdges[ei.edge.idx]; !ok {
				return false
			}
		}
	}
	return true
 }
