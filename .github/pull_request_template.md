## What and why

<!-- What does this change, and why. Link an issue if there is one — required
     for anything touching storage format, capture engine internals, or
     fencing (see CONTRIBUTING.md's "what needs an issue first"). -->

## Checklist

- [ ] Tests added or updated for this change
- [ ] `go test ./... -race` passes locally
- [ ] `make test-torture` run and passing — **required** if this touches
      `internal/capture` or a flush path in `internal/session`; leave
      unchecked and say why if it doesn't apply
- [ ] Docs updated (README, ROADMAP, or code comments — whichever describes
      what changed), or this change doesn't need it
- [ ] Every commit is signed off (`git commit -s`) per the DCO — see
      CONTRIBUTING.md
