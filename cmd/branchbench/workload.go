package main

import (
	"database/sql"
	"fmt"
	"math/rand"
)

// workflow is one BranchBench macrobenchmark topology: the parameter tuple
// (T, S, F_r, F_i, D, C, gamma, M_s, M_d, Q_v) plus a name. The values are
// BranchBench's own full-scale numbers (arXiv:2604.17180, Table 1 /
// Section 3.3, as committed in its harness's macrobench configs); only the
// numbers are reused here — no file, schema or SQL from that harness is.
type workflow struct {
	name        string
	shape       string
	workers     int // T
	steps       int // S, per worker
	rootFanout  int // F_r
	innerFanout int // F_i (0 where D=1 makes inner levels unreachable)
	maxDepth    int // D
	crossBranch int // C, whole-run cross-branch queries
	prune       float64
	schemaOps   int // M_s per step
	mutations   int // M_d per step
	evals       int // Q_v per step
}

// workflows is the full-scale table, in the order the runner walks it.
var workflows = []workflow{
	{name: "simulation", shape: "flat star", workers: 1000, steps: 1, rootFanout: 1000, innerFanout: 0, maxDepth: 1, crossBranch: 1, prune: 1.0, schemaOps: 0, mutations: 50, evals: 1},
	{name: "data_cleaning", shape: "wide shallow", workers: 10, steps: 20, rootFanout: 10, innerFanout: 3, maxDepth: 3, crossBranch: 2, prune: 0.0, schemaOps: 1, mutations: 1, evals: 1},
	{name: "software_dev", shape: "bushy", workers: 5, steps: 20, rootFanout: 5, innerFanout: 3, maxDepth: 4, crossBranch: 1, prune: 0.1, schemaOps: 1, mutations: 1, evals: 2},
	{name: "mcts", shape: "deep narrow", workers: 10, steps: 100, rootFanout: 10, innerFanout: 10, maxDepth: 25, crossBranch: 0, prune: 0.1, schemaOps: 0, mutations: 1, evals: 1},
	{name: "failure_repro", shape: "flat, 1 worker", workers: 1, steps: 10, rootFanout: 10, innerFanout: 0, maxDepth: 1, crossBranch: 0, prune: 1.0, schemaOps: 5, mutations: 45, evals: 1},
}

// quick scales each workflow down so the whole table runs in seconds (the
// -quick flag, and what cmd/branchbench's test uses). Only T/S/D shrink;
// the per-step op mix, fanouts and prune probability are untouched, so the
// tree shapes stay the shapes.
func (w workflow) quick() workflow {
	switch w.name {
	case "simulation":
		w.workers, w.rootFanout = 20, 20
	case "data_cleaning":
		w.workers, w.steps = 3, 4
	case "software_dev":
		w.workers, w.steps = 2, 4
	case "mcts":
		w.workers, w.steps, w.maxDepth = 2, 6, 5
	case "failure_repro":
		w.steps = 3
	}
	return w
}

// branchPrefix is the store-name-safe prefix a workflow's branches carry
// (store.ValidateName allows [a-z0-9-_.] only).
func (w workflow) branchPrefix() string {
	switch w.name {
	case "simulation":
		return "sim"
	case "data_cleaning":
		return "clean"
	case "software_dev":
		return "dev"
	case "failure_repro":
		return "repro"
	default:
		return w.name
	}
}

func allWorkflowNames() []string {
	names := make([]string, 0, len(workflows))
	for _, w := range workflows {
		names = append(names, w.name)
	}
	return names
}

func workflowByName(name string) (workflow, bool) {
	for _, w := range workflows {
		if w.name == name {
			return w, true
		}
	}
	return workflow{}, false
}

// applySchemaChange runs one M_s schema change against a branch checkout.
// n comes from a run-wide counter, so a column added on a child can never
// collide with one its parent already added (a per-step counter would
// collide down any chain deeper than one).
func applySchemaChange(db *sql.DB, n int) error {
	if n%2 == 1 {
		_, err := db.Exec(fmt.Sprintf(`ALTER TABLE customer ADD COLUMN c_x%d INTEGER DEFAULT 0`, n))
		return err
	}
	if _, err := db.Exec(fmt.Sprintf(`DROP INDEX IF EXISTS idx_%d`, n)); err != nil {
		return err
	}
	_, err := db.Exec(fmt.Sprintf(`CREATE INDEX idx_%d ON order_line (ol_w_id, ol_d_id)`, n))
	return err
}

// applyMutation runs one M_d data mutation: a TPC-C-shaped "new order" —
// one orders row, 5-15 order_line rows, one stock decrement — in a single
// transaction.
func applyMutation(db *sql.DB, r *rand.Rand, orderID, warehouses int) error {
	w := 1 + r.Intn(warehouses)
	d := 1 + r.Intn(districtsPerWarehouse)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	lines := 5 + r.Intn(11)
	if _, err := tx.Exec(`INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)`,
		w, d, orderID, 1+r.Intn(customersPerDistrict), "2026-09-26T00:00:00Z", 0, lines, 1); err != nil {
		tx.Rollback()
		return err
	}
	for n := 1; n <= lines; n++ {
		if _, err := tx.Exec(`INSERT INTO order_line VALUES (?,?,?,?,?,?,?,?,?,?)`,
			w, d, orderID, n, 1+r.Intn(itemCount), w, "2026-09-26T00:00:00Z", 1+r.Intn(10), money(r, 100), text(r, 24)); err != nil {
			tx.Rollback()
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE stock SET s_quantity = s_quantity - 1 WHERE s_w_id = ? AND s_i_id = ?`,
		w, 1+r.Intn(stockPerWarehouse)); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// evalSum is the branch-local Q_v eval query every workflow runs: the
// per-warehouse order-line total on the branch just mutated.
const evalSum = `SELECT ol_w_id, SUM(ol_amount) FROM order_line GROUP BY ol_w_id`

// evalIntegrity is software-dev's second eval query (its op mix is
// DDL-heavy and its Q_v is 2): an orphaned-order_line integrity check, the
// "integrity test" that workflow's description calls for.
const evalIntegrity = `SELECT COUNT(*) FROM order_line ol LEFT JOIN orders o ` +
	`ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id WHERE o.o_id IS NULL`

// evalQuery picks the i-th eval query for a workflow.
func evalQuery(w workflow, i int) string {
	if w.name == "software_dev" && i == 1 {
		return evalIntegrity
	}
	return evalSum
}

// runQuery executes q and drains its rows (so the scan cost is paid, not
// deferred).
func runQuery(db *sql.DB, q string) error {
	rows, err := db.Query(q)
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	return rows.Close()
}
