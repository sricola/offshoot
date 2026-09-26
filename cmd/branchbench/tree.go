package main

import (
	"math/rand"
	"sync"
)

// node is one branch in a workflow's tree. children counts LIVE children
// only (a pruned child gives its slot back), and ready is false until the
// node's own step has finished — an unfinished node is never chosen as a
// parent, which is also what keeps Fork's parent-side quiesce off a
// checkout another goroutine is still writing.
type node struct {
	name     string
	parent   *node
	depth    int
	children int
	ready    bool
	alive    bool
}

type tree struct {
	mu    sync.Mutex
	rng   *rand.Rand
	wf    workflow
	root  *node
	nodes []*node
	live  int // alive, ready or not, excluding the root
	peak  int
	// rootFallbacks counts the steps that had to fork from the root because
	// no node was eligible any more — the last clause of BranchBench's own
	// parent-selection rule, which a tuple whose T x S exceeds the capacity
	// its fanouts and depth allow (data_cleaning: 10 + 30 + 90 = 130 nodes
	// for 200 steps) is guaranteed to reach.
	rootFallbacks int
}

func newTree(wf workflow) *tree {
	root := &node{name: "main", depth: 0, ready: true, alive: true}
	return &tree{rng: rand.New(rand.NewSource(7)), wf: wf, root: root, nodes: []*node{root}}
}

func (t *tree) fanout(n *node) int {
	if n.depth == 0 {
		return t.wf.rootFanout
	}
	return t.wf.innerFanout
}

func (t *tree) eligible(n *node) bool {
	return n != nil && n.alive && n.ready && n.depth < t.wf.maxDepth && n.children < t.fanout(n)
}

// reserve picks this step's parent per BranchBench's tree loop — the
// worker's own current node when it still has room and depth, else a
// uniformly random eligible node, else the root — and attaches a
// not-yet-ready child to it, holding the slot for the duration of the step.
func (t *tree) reserve(cur *node, name string) *node {
	t.mu.Lock()
	defer t.mu.Unlock()
	parent := cur
	if !t.eligible(parent) {
		var options []*node
		for _, n := range t.nodes {
			if t.eligible(n) {
				options = append(options, n)
			}
		}
		if len(options) > 0 {
			parent = options[t.rng.Intn(len(options))]
		} else {
			parent = t.root
			t.rootFallbacks++
		}
	}
	child := &node{name: name, parent: parent, depth: parent.depth + 1, alive: true}
	parent.children++
	t.nodes = append(t.nodes, child)
	t.live++
	if t.live > t.peak {
		t.peak = t.live
	}
	return child
}

// publish marks a finished node forkable.
func (t *tree) publish(n *node) {
	t.mu.Lock()
	n.ready = true
	t.mu.Unlock()
}

// prune removes a destroyed node and returns its parent's slot.
func (t *tree) prune(n *node) {
	t.mu.Lock()
	n.alive = false
	n.parent.children--
	t.live--
	t.mu.Unlock()
}

// liveBranches lists every branch a cross-branch query should touch: the
// root plus every surviving forked node.
func (t *tree) liveBranches() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for _, n := range t.nodes {
		if n.alive && n.ready {
			out = append(out, n.name)
		}
	}
	return out
}
