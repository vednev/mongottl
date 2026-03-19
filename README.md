# MongoDB Batch Deleter

TTL-style batch delete service for MongoDB. Deletes one batch of expired documents per invocation, throttling the batch size based on real-time cluster health signals. Scheduling is left entirely to the caller.

## Requirements

- Python 3.11+
- `pymongo`, `httpx`
- `boto3` — only required in Lambda mode (fetching the URI from Secrets Manager)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Modes

### Default mode

Reads config from CLI arguments, connects using `--uri`, runs one delete cycle, and exits. The caller is responsible for scheduling (cron, systemd timer, etc.).

```
python main.py \
    --uri       "mongodb+srv://user:pass@cluster.mongodb.net" \
    --db        mydb \
    --coll      mycoll \
    --field     createdAt \
    --ttl       2592000 \
    [--max-batch 1000] \
    [--repl-lag-hard-stop 200] \
    [--dirty-scale-start 0.10] \
    [--dirty-hard-stop 0.20] \
    [--atlas-group-id <id>] \
    [--atlas-public-key <key>] \
    [--atlas-private-key <key>] \
    [--atlas-cluster-name <name>] \
    [--dry-run]
```

### Lambda mode

Reads config from environment variables, fetches the MongoDB URI from AWS Secrets Manager (cached after the first call), and runs one delete cycle. The `handler(event, context)` function is the Lambda entry point.

Pass `--lambda` on the CLI to invoke the Lambda code path locally for testing:

```bash
MONGO_SECRET_NAME=mongo/batch-deleter \
MONGO_DB=mydb \
MONGO_COLLECTION=mycoll \
MONGO_FIELD=createdAt \
MONGO_TTL_SECONDS=2592000 \
python main.py --lambda
```

## Arguments (default mode)

| Argument | Required | Default | Description |
|---|---|---|---|
| `--uri` | yes | — | MongoDB connection URI |
| `--db` | yes | — | Database name |
| `--coll` | yes | — | Collection name |
| `--field` | yes | — | Date field used to determine document age |
| `--ttl` | yes | — | Retention period in seconds. Documents older than this are eligible for deletion. |
| `--max-batch` | no | `1000` | Maximum number of documents to delete per run. |
| `--repl-lag-hard-stop` | no | `200` | Replication lag in seconds that triggers a hard stop. |
| `--dirty-scale-start` | no | `0.10` | Dirty cache ratio (0–1) at which batch scaling begins. |
| `--dirty-hard-stop` | no | `0.20` | Dirty cache ratio (0–1) at which batch scaling reaches zero. |
| `--atlas-group-id` | no* | — | Atlas project Group ID. *Required for sharded Atlas clusters. |
| `--atlas-public-key` | no* | — | Atlas API public key (HTTP Digest auth). *Required for sharded clusters. |
| `--atlas-private-key` | no* | — | Atlas API private key (HTTP Digest auth). *Required for sharded clusters. |
| `--atlas-cluster-name` | no* | — | Atlas cluster name. *Required for sharded clusters. |
| `--dry-run` | no | `false` | Log what would be deleted without actually deleting. |

## How it works

Each run:

1. Samples cluster health signals (see below)
2. If a hard stop is active, exits immediately without deleting anything
3. Computes an effective batch size based on current utilization
4. Fetches up to `batch_size` document IDs (oldest first, sorted by `--field`) via an aggregation pipeline and deletes them with `delete_many`
5. Returns a result dict (logged in both modes; returned as the Lambda response in Lambda mode)

## Throttling signals

### Hard stops

Any of the following causes the run to be skipped entirely:

| Signal | Source | Threshold |
|---|---|---|
| Replication lag | `replSetGetStatus` (replica set) or `OPLOG_REPLICATION_LAG` (Atlas API) | >= `--repl-lag-hard-stop` (default 200s) |
| Write queue depth | `queues.execution.write.normalPriority.queueLength` (replica set) or `GLOBAL_LOCK_CURRENT_QUEUE_WRITERS` (Atlas API) | > 0 |
| Disk queue depth | `DISK_QUEUE_DEPTH` via Atlas API — current vs. daily max | current >= daily max |

The disk queue depth hard stop applies to sharded Atlas clusters only.

### Gradual scaling

When no hard stop is active, the batch size scales down linearly based on the worst of two signals:

| Signal | Source | Scale range |
|---|---|---|
| Replication lag | As above | 50% of `--repl-lag-hard-stop` → 100% (default 100s → 200s) |
| Dirty cache ratio | `wiredTiger.cache` (replica set) or `DIRTY_FILL_RATIO` (Atlas API) | `--dirty-scale-start` → `--dirty-hard-stop` (default 10% → 20%) |

Each signal is normalized to a 0–1 score. The higher score drives the batch size: `batch_size = max_batch × (1 − score)`.

## Topology

### Replica sets

Metrics sampled directly from the primary via `serverStatus` and `replSetGetStatus`. No Atlas API credentials required.

### Sharded Atlas clusters

Metrics fetched from the Atlas Admin API (`--atlas-*` args required). No direct shard connections are made; the `directShardOperations` role is not required.

Processes belonging to the cluster are identified by `replicaSetName` containing `--atlas-cluster-name` (e.g. `MyCluster-shard-0`) or `shardName` matching it exactly. The worst metric value across all processes governs throttling.

## Notes

- An index on `--field` is strongly recommended.
- Writes use `w: majority` — deletions are durably acknowledged on a majority of nodes before the function returns.
- The service holds no local state. It is safe to run concurrently from multiple hosts, though doing so will increase the delete rate.
