// Command offshoot is the branchable-SQLite CLI (Plan 2: local mode).
package main

import (
	"fmt"
	"os"

	"github.com/offshoot-db/offshoot/internal/ops"
)

const usage = `offshoot — branch SQLite like git (local mode)

Usage:
  offshoot init                      create a store in ./.offshoot
  offshoot create <db> [--from f]    new database (branch main), or import file f
  offshoot checkout <db>[@branch]    materialize a working copy; prints its path
  offshoot checkpoint <db>[@branch] <name>   snapshot the checkout as a named checkpoint
  offshoot fork <db>[@branch] <new> [--at cp]   branch from head or a checkpoint
  offshoot path <db>[@branch]        print the checkout path

Store location: -store DIR or OFFSHOOT_STORE, default ./.offshoot
`

func storeRoot(args []string) (string, []string) {
	root := os.Getenv("OFFSHOOT_STORE")
	if root == "" {
		root = ".offshoot"
	}
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == "-store" && i+1 < len(args) {
			root = args[i+1]
			i++
			continue
		}
		out = append(out, args[i])
	}
	return root, out
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "offshoot:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	root, args := storeRoot(args)
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	cmd, rest := args[0], args[1:]

	if cmd == "init" {
		_, err := ops.Init(root)
		if err == nil {
			fmt.Println("initialized store at", root)
		}
		return err
	}

	w, err := ops.Open(root)
	if err != nil {
		return fmt.Errorf("open store %s: %w (run 'offshoot init'?)", root, err)
	}
	switch cmd {
	case "create":
		if len(rest) < 1 {
			return fmt.Errorf("usage: offshoot create <db> [--from file]")
		}
		if len(rest) == 3 && rest[1] == "--from" {
			return w.CreateFrom(rest[0], rest[2])
		}
		return w.Create(rest[0])
	case "checkpoint":
		if len(rest) != 2 {
			return fmt.Errorf("usage: offshoot checkpoint <db>[@branch] <name>")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		txid, err := w.Checkpoint(db, branch, rest[1])
		if err != nil {
			return err
		}
		fmt.Printf("checkpoint %q at txid %d\n", rest[1], txid)
		return nil
	case "fork":
		fs := rest
		at := ""
		if len(fs) == 4 && fs[2] == "--at" {
			at = fs[3]
			fs = fs[:2]
		}
		if len(fs) != 2 {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		txid, err := w.Fork(db, branch, fs[1], at)
		if err != nil {
			return err
		}
		fmt.Printf("forked %s@%s -> %s@%s at txid %d\n", db, branch, db, fs[1], txid)
		return nil
	case "checkout", "path":
		if len(rest) != 1 {
			return fmt.Errorf("usage: offshoot %s <db>[@branch]", cmd)
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		if cmd == "path" {
			fmt.Println(w.CheckoutPath(db, branch))
			return nil
		}
		path, err := w.Checkout(db, branch)
		if err != nil {
			return err
		}
		fmt.Println(path)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
}
