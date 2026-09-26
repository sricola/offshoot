package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/sricola/offshoot/internal/ops"
)

// seedDB is the one database every workflow forks from. Each workflow forks
// its whole tree off db@main, whose head never moves after the seed
// checkpoint below — so all five workflows start from byte-identical state.
const seedDB = "chbench"

// schemaDDL is a CH-benCHmark-SHAPED schema: TPC-C's transactional tables
// plus TPC-H's region/nation/supplier dimension tables, the combination the
// CH-benCHmark (Cole et al., DBTest 2011) defines and BranchBench's
// macrobenchmarks run against. It is written from the column vocabulary of
// TPC-C/TPC-H, not copied from BranchBench's harness (which carries no
// license — see docs/benchmarks.md): types are SQLite-native, and the wide
// filler columns (c_data, s_data, i_data) are shorter than TPC-C's, which
// keeps the seed in the low tens of MiB.
var schemaDDL = []string{
	`CREATE TABLE region (r_regionkey INTEGER PRIMARY KEY, r_name TEXT, r_comment TEXT)`,
	`CREATE TABLE nation (n_nationkey INTEGER PRIMARY KEY, n_name TEXT, n_regionkey INTEGER, n_comment TEXT)`,
	`CREATE TABLE supplier (su_suppkey INTEGER PRIMARY KEY, su_name TEXT, su_address TEXT, su_nationkey INTEGER, su_phone TEXT, su_acctbal REAL, su_comment TEXT)`,
	`CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT, w_tax REAL, w_ytd REAL)`,
	`CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_name TEXT, d_street_1 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT, d_tax REAL, d_ytd REAL, d_next_o_id INTEGER, PRIMARY KEY (d_w_id, d_id))`,
	`CREATE TABLE customer (c_w_id INTEGER, c_d_id INTEGER, c_id INTEGER, c_first TEXT, c_middle TEXT, c_last TEXT, c_street_1 TEXT, c_street_2 TEXT, c_city TEXT, c_state TEXT, c_zip TEXT, c_phone TEXT, c_since TEXT, c_credit TEXT, c_credit_lim REAL, c_discount REAL, c_balance REAL, c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id))`,
	`CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)`,
	`CREATE TABLE stock (s_w_id INTEGER, s_i_id INTEGER, s_quantity INTEGER, s_dist_01 TEXT, s_dist_02 TEXT, s_dist_03 TEXT, s_dist_04 TEXT, s_dist_05 TEXT, s_dist_06 TEXT, s_dist_07 TEXT, s_dist_08 TEXT, s_dist_09 TEXT, s_dist_10 TEXT, s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, PRIMARY KEY (s_w_id, s_i_id))`,
	`CREATE TABLE orders (o_w_id INTEGER, o_d_id INTEGER, o_id INTEGER, o_c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id))`,
	`CREATE TABLE new_order (no_w_id INTEGER, no_d_id INTEGER, no_o_id INTEGER, PRIMARY KEY (no_w_id, no_d_id, no_o_id))`,
	`CREATE TABLE history (h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)`,
	`CREATE TABLE order_line (ol_w_id INTEGER, ol_d_id INTEGER, ol_o_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`,
}

const (
	districtsPerWarehouse = 10
	customersPerDistrict  = 100
	itemCount             = 1000
	stockPerWarehouse     = 1000
	// mutationOrderBase keeps every "new order" a workflow step inserts far
	// above the seed's own o_id range, so a child branch can never collide
	// with an order its parent already holds.
	mutationOrderBase = 1000000
)

// buildSeed creates db@main, writes the CH-benCHmark-shaped rows into its
// checkout, checkpoints it as "seed", and returns the checkout file's size
// in bytes (the number reported as "seed N MiB").
func buildSeed(ws *ops.Workspace, warehouses int) (int64, error) {
	if err := ws.Create(seedDB); err != nil {
		return 0, err
	}
	path, err := ws.Checkout(seedDB, "main")
	if err != nil {
		return 0, err
	}
	if err := writeSeedRows(path, warehouses); err != nil {
		return 0, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if _, err := ws.Checkpoint(seedDB, "main", "seed", nil); err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// writeSeedRows fills a freshly materialized checkout. Deterministic:
// math/rand seeded with 1, single goroutine, fixed insert order. journal
// mode is switched off WAL so the whole database lives in the one file the
// store snapshots (and so the reported seed size is the whole database);
// synchronous=OFF because durability of a scratch checkout is not what is
// being measured.
func writeSeedRows(path string, warehouses int) error {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF`); err != nil {
		db.Close()
		return err
	}
	for _, stmt := range schemaDDL {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return fmt.Errorf("branchbench: seed DDL %.40q: %w", stmt, err)
		}
	}
	r := rand.New(rand.NewSource(1))
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return err
	}
	if err := insertSeedRows(tx, r, warehouses); err != nil {
		tx.Rollback()
		db.Close()
		return err
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		return err
	}
	return db.Close()
}

func insertSeedRows(tx *sql.Tx, r *rand.Rand, warehouses int) error {
	for i := 1; i <= 5; i++ {
		if _, err := tx.Exec(`INSERT INTO region VALUES (?,?,?)`, i, text(r, 12), text(r, 40)); err != nil {
			return err
		}
	}
	for i := 1; i <= 25; i++ {
		if _, err := tx.Exec(`INSERT INTO nation VALUES (?,?,?,?)`, i, text(r, 14), 1+r.Intn(5), text(r, 40)); err != nil {
			return err
		}
	}
	for i := 1; i <= 100; i++ {
		if _, err := tx.Exec(`INSERT INTO supplier VALUES (?,?,?,?,?,?,?)`,
			i, text(r, 20), text(r, 24), 1+r.Intn(25), text(r, 16), money(r, 10000), text(r, 60)); err != nil {
			return err
		}
	}
	for i := 1; i <= itemCount; i++ {
		if _, err := tx.Exec(`INSERT INTO item VALUES (?,?,?,?,?)`, i, r.Intn(10000), text(r, 20), money(r, 100), text(r, 40)); err != nil {
			return err
		}
	}
	stockStmt, err := tx.Prepare(`INSERT INTO stock VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	custStmt, err := tx.Prepare(`INSERT INTO customer VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	ordStmt, err := tx.Prepare(`INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	lineStmt, err := tx.Prepare(`INSERT INTO order_line VALUES (?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	for w := 1; w <= warehouses; w++ {
		if _, err := tx.Exec(`INSERT INTO warehouse VALUES (?,?,?,?,?,?,?,?,?)`,
			w, text(r, 10), text(r, 20), text(r, 20), text(r, 20), text(r, 2), text(r, 9), 0.1, 0.0); err != nil {
			return err
		}
		for i := 1; i <= stockPerWarehouse; i++ {
			if _, err := stockStmt.Exec(w, i, 10+r.Intn(90),
				text(r, 24), text(r, 24), text(r, 24), text(r, 24), text(r, 24),
				text(r, 24), text(r, 24), text(r, 24), text(r, 24), text(r, 24),
				0, 0, 0, text(r, 50)); err != nil {
				return err
			}
		}
		for d := 1; d <= districtsPerWarehouse; d++ {
			next := customersPerDistrict + 1
			if _, err := tx.Exec(`INSERT INTO district VALUES (?,?,?,?,?,?,?,?,?,?)`,
				w, d, text(r, 10), text(r, 20), text(r, 20), text(r, 2), text(r, 9), 0.1, 0.0, next); err != nil {
				return err
			}
			for c := 1; c <= customersPerDistrict; c++ {
				if _, err := custStmt.Exec(w, d, c, text(r, 16), "oe", text(r, 16),
					text(r, 20), text(r, 20), text(r, 20), text(r, 2), text(r, 9), text(r, 16),
					"2026-01-01T00:00:00Z", "GC", 50000.0, 0.05, -10.0, 10.0, 1, 0, text(r, 100)); err != nil {
					return err
				}
				if _, err := tx.Exec(`INSERT INTO history VALUES (?,?,?,?,?,?,?,?)`,
					c, d, w, d, w, "2026-01-01T00:00:00Z", 10.0, text(r, 24)); err != nil {
					return err
				}
				// One order per customer, 5-15 lines each.
				if _, err := ordStmt.Exec(w, d, c, c, "2026-01-01T00:00:00Z", 1+r.Intn(10), 0, 1); err != nil {
					return err
				}
				lines := 5 + r.Intn(11)
				if _, err := tx.Exec(`UPDATE orders SET o_ol_cnt = ? WHERE o_w_id=? AND o_d_id=? AND o_id=?`, lines, w, d, c); err != nil {
					return err
				}
				for n := 1; n <= lines; n++ {
					if _, err := lineStmt.Exec(w, d, c, n, 1+r.Intn(itemCount), w,
						"2026-01-01T00:00:00Z", 1+r.Intn(10), money(r, 100), text(r, 24)); err != nil {
						return err
					}
				}
				if c > customersPerDistrict*7/10 { // last 30%: still "new"
					if _, err := tx.Exec(`INSERT INTO new_order VALUES (?,?,?)`, w, d, c); err != nil {
						return err
					}
				}
			}
		}
	}
	for _, s := range []*sql.Stmt{stockStmt, custStmt, ordStmt, lineStmt} {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

const textAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// text returns an n-character pseudo-random filler string.
func text(r *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(textAlphabet[r.Intn(len(textAlphabet))])
	}
	return b.String()
}

// money returns a pseudo-random amount in [0, max) rounded to cents.
func money(r *rand.Rand, max int) float64 {
	return float64(r.Intn(max*100)) / 100
}
