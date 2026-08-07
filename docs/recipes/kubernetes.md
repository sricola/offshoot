# Recipe: Kubernetes sidecar

**Scope check first:** this is a recipe for running ONE `offshoot serve`
process per pod, as a sidecar next to your agent container — not a
clustering story. offshoot has no multi-node mode: no placement, no
failover, no cross-node routing (see [ROADMAP.md's
non-goals](../../ROADMAP.md#non-goals-v1) and the FAQ's [why not
LiteFS](../faq.md#why-not-litefs)). There is deliberately no
StatefulSet-for-HA pattern below — running more than one `offshoot serve`
replica against the same store from different pods is not something this
codebase coordinates for you beyond the same lease/fencing safety it always
provides (a second replica trying to write the same branch loses the race
exactly like a second process on one host would). If you need HA today,
run your agent workload with `replicas: 1` per store and accept a restart on
pod failure, the same trade-off you'd make running `offshoot serve` as a
single systemd unit.

## Why the socket AND the checkouts must be co-located

The daemon's unix socket is the easy part — any two containers in the same
Pod already share a network namespace, so binding `-http 127.0.0.1:PORT`
in the offshoot container is reachable from the agent container's own
`localhost` too. The checkouts are the part that's easy to get wrong: a
checkout is a **real SQLite file on a real filesystem path**, opened
directly by `sqlite3`/your driver in the agent's own process — not proxied
through offshoot at all. offshoot's job ends at handing back a path
(`session open` / `Client.open()`); after that, the agent process does its
own `open()`/`read()`/`write()` syscalls against that path. A SQLite file
cannot be opened over a network filesystem safely (locking semantics don't
hold up), so the checkout path has to resolve to the same physical
filesystem the agent container sees — which in Kubernetes means the same
Pod, sharing a volume, not two pods each mounting "the same" networked
disk. `emptyDir` is the right primitive: ephemeral, pod-local, and shared
by every container in that Pod by construction.

Both the socket and the checkout tree go on that same `emptyDir` for the
same underlying reason: they're both real filesystem objects the daemon
serves in this one pod's local namespace, and neither should be assumed
durable — `emptyDir`'s contents die with the pod, which is fine, because
durability lives in the *store* (S3-compatible bucket, or a separately
mounted volume for the local backend), never in the checkout cache.

## The manifest

This is a real, apply-able manifest — not pseudocode. It uses a local
directory store mounted from a `PersistentVolumeClaim` in this example (swap
`OFFSHOOT_STORE` for an `s3://` spec plus credentials if you're on S3/R2/
Tigris/MinIO instead; either way, `OFFSHOOT_CHECKOUTS` and the socket path
stay on the pod-local `emptyDir`, never the store volume, per
[docs/reference.md](../reference.md)'s `OFFSHOOT_CHECKOUTS` note that a
remote store's checkouts always live outside the store itself).

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: offshoot-http-token
type: Opaque
stringData:
  # Illustrative value only — generate a real one, e.g.:
  #   kubectl create secret generic offshoot-http-token \
  #     --from-literal=token="$(openssl rand -hex 32)"
  token: "REPLACE-ME-WITH-A-REAL-TOKEN"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: offshoot-agent
  labels:
    app: offshoot-agent
spec:
  replicas: 1 # single-node scope — see docs/recipes/kubernetes.md's top note; do not scale this up
  strategy:
    type: Recreate # a second live replica would just race the first for every lease
  selector:
    matchLabels:
      app: offshoot-agent
  template:
    metadata:
      labels:
        app: offshoot-agent
      annotations:
        # Only scraped if the offshoot container below is configured for a
        # non-loopback bind (see docs/recipes/kubernetes.md's "Metrics
        # scraping" section) — with the loopback-only default below this
        # annotation is inert.
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      volumes:
        - name: offshoot-runtime
          emptyDir: {} # socket + checkouts; pod-local, ephemeral by design
        - name: offshoot-store
          persistentVolumeClaim:
            claimName: offshoot-store-pvc # swap for an s3:// store + Secret; see the recipe doc
      containers:
        - name: offshoot
          image: ghcr.io/offshoot-db/offshoot:latest # illustrative tag — pin a real release
          args:
            - serve
            - -socket=/offshoot/run/o.sock
            - -http=127.0.0.1:9090
            - -flush-every=30s
            - -ro-cache-budget=1GB
          env:
            - name: OFFSHOOT_STORE
              value: /offshoot/store
            - name: OFFSHOOT_CHECKOUTS
              value: /offshoot/run/checkouts
            - name: OFFSHOOT_SOCKET
              value: /offshoot/run/o.sock
            - name: OFFSHOOT_TOKEN
              valueFrom:
                secretKeyRef:
                  name: offshoot-http-token
                  key: token
          volumeMounts:
            - name: offshoot-runtime
              mountPath: /offshoot/run
            - name: offshoot-store
              mountPath: /offshoot/store
          # httpGet probes are evaluated by the kubelet against the Pod IP,
          # not the container's own loopback — with -http bound to
          # 127.0.0.1 (the safe default; see the recipe doc's "Metrics
          # scraping" section), an httpGet probe here would never actually
          # reach it. exec runs inside this container's own network
          # namespace, so it sees the same loopback the offshoot process
          # bound to.
          livenessProbe:
            exec:
              command: ["/bin/sh", "-c", "wget -q -O- http://127.0.0.1:9090/healthz | grep -q '\"ok\":true'"]
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            exec:
              command: ["/bin/sh", "-c", "wget -q -O- http://127.0.0.1:9090/healthz | grep -q '\"ok\":true'"]
            initialDelaySeconds: 2
            periodSeconds: 5
        - name: agent
          image: your-agent-image:latest # illustrative — your workload's own image
          env:
            - name: OFFSHOOT_SOCKET
              value: /offshoot/run/o.sock
          volumeMounts:
            - name: offshoot-runtime
              mountPath: /offshoot/run
          command: ["/bin/sh", "-c", "sleep infinity"] # placeholder for your real entrypoint
```

Notes on the illustrative pieces above, spelled out rather than left to
guesswork:

- `ghcr.io/offshoot-db/offshoot:latest` and `your-agent-image:latest` are
  placeholders — offshoot does not (yet) publish a container image as part
  of this milestone; see [docs/status.md](../status.md)'s publish-pipeline
  row for what *is* actually published today (source + SDKs). Build your
  own image from this repo's `cmd/offshoot` until an official one exists.
- The `offshoot-store-pvc` `PersistentVolumeClaim` referenced isn't created
  by this manifest — either provision one (`kubectl get storageclass` for
  what's available in your cluster) or switch `OFFSHOOT_STORE` to an
  `s3://bucket/prefix` spec plus the usual AWS SDK credential env vars
  (`OFFSHOOT_S3_ENDPOINT`/`OFFSHOOT_S3_REGION`/`OFFSHOOT_S3_PATH_STYLE` for
  R2/Tigris/MinIO — see [docs/reference.md](../reference.md)'s S3
  environment variables table) if you don't want a local-directory store at
  all. Either way, `OFFSHOOT_CHECKOUTS` stays on the `emptyDir`.

### Metrics scraping

The manifest above binds `-http 127.0.0.1:9090` — **loopback only, reachable
from inside this one Pod's network namespace and nowhere else**, matching
the same "off by default beyond localhost" posture the daemon has
everywhere else (see [docs/operations.md](../operations.md#httpauth-threat-model)).
That's deliberately the safer default here too: a Pod IP is routable by
every other Pod in the cluster network, and offshoot's own threat model
treats any non-loopback bind as requiring an explicit, acknowledged opt-in
— Kubernetes doesn't get an exception from that rule just because it "feels"
internal.

Two honest options, not one glossed-over one:

1. **Stay loopback-only (the manifest above).** The `prometheus.io/scrape`
   annotation is present for documentation purposes but won't actually be
   reachable by a Prometheus instance running as its own Pod — only
   something running *inside this same Pod* (an `exec`-based scrape, or a
   sidecar metrics-relay container you add yourself) can reach it. This is
   the right choice if you don't need cluster-wide scraping and just want
   `/healthz` for probes and occasional `kubectl exec ... wget
   127.0.0.1:9090/metrics` spot checks.
2. **Opt in to non-loopback, deliberately.** Change the offshoot
   container's args to `-http=0.0.0.0:9090 -http-allow-non-loopback` (both
   required together — see [docs/reference.md](../reference.md)'s `-http
   ADDR` section for the exact two-error-message behavior if you only set
   one) and keep `OFFSHOOT_TOKEN` set from the Secret as the manifest
   already does. Now the `prometheus.io/scrape` annotation is real, and a
   Prometheus scrape config needs the token:

   ```yaml
   scrape_configs:
     - job_name: offshoot
       kubernetes_sd_configs:
         - role: pod
       authorization:
         type: Bearer
         credentials_file: /etc/prometheus-secrets/offshoot-token # mount the same Secret
       relabel_configs:
         - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
           action: keep
           regex: "true"
         - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
           action: replace
           target_label: __address__
           regex: (.+)
           replacement: "${1}"
   ```

   Choosing option 2 widens the token's blast radius to "anyone who can
   reach this Pod's IP inside the cluster network," which is a real,
   deliberate trade — not a mistake to sweep under the rug. Prefer option 1
   unless you actually need cluster-wide scraping.

### Liveness/readiness

Both probes above hit `GET /healthz` — the one route that needs no token —
via `exec` rather than `httpGet`, for the reason spelled out in the
manifest's own comment: `httpGet` probes are dispatched by the kubelet
against the Pod's IP, which never reaches a `127.0.0.1`-bound listener; an
`exec` probe runs inside the container's own network namespace and sees the
same loopback the process bound to. If you switch to the non-loopback
option above, an `httpGet` probe against `/healthz` on the Pod IP works
fine too — either shape is a valid choice once the bind address matches
what's actually probing it.

## Validating the manifest

`kubectl apply --dry-run=client` needs API discovery even for a purely
client-side check, which means it needs *some* reachable API server — there
was none configured in the environment this doc was written in, so this was
validated against a real, disposable one instead: a `rancher/k3s` control
plane started fresh in Docker for exactly this check, torn down
immediately after. Real output, both dry-run modes, no image pull for the
Deployment's own containers (dry-run never schedules a Pod):

```
$ kubectl get nodes
NAME           STATUS   ROLES                  AGE   VERSION
db941095b854   Ready    control-plane,master   9s    v1.29.4+k3s1

$ kubectl apply --dry-run=client -f docs/recipes/k8s/offshoot-sidecar.yaml
secret/offshoot-http-token created (dry run)
deployment.apps/offshoot-agent created (dry run)

$ kubectl apply --dry-run=server -f docs/recipes/k8s/offshoot-sidecar.yaml
secret/offshoot-http-token created (server dry run)
deployment.apps/offshoot-agent created (server dry run)
```

`--dry-run=server` is the stronger check — the manifest round-trips through
a real API server's admission chain, not just local schema validation — and
it passed too. If you don't have Docker or `kubectl` on hand, `python3 -c
"import yaml, sys; list(yaml.safe_load_all(sys.stdin))"` (or any YAML-syntax
validator) at least confirms the file parses as well-formed YAML — a
strictly weaker check than either dry-run above.

## What this recipe does not claim

- No StatefulSet, no `PodDisruptionBudget` tuned for HA, no multi-replica
  story — see the top of this page.
- No official offshoot container image yet — build your own from this
  repo until [docs/status.md](../status.md) says otherwise.
- No admission-controller / NetworkPolicy guidance — write the
  `NetworkPolicy` your cluster's security posture requires around this Pod;
  that's environment-specific and out of scope for a single-binary tool's
  own docs.
