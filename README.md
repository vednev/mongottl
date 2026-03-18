# MongoDB Batch Deleter

A long-running TTL-style delete service for MongoDB. Deletes expired documents in batches, throttling itself each cycle based on real-time cluster health signals.

## Requirements

- Python 3.8+
- `pymongo`

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```
python main.py \
    --uri       "mongodb+srv://user:pass@cluster.mongodb.net" \
    --db        mydb \
    --coll      mycoll \
    --field     createdAt \
    --ttl       2592000 \
    [--max-batch 1000] \
    [--interval 60] \
    [--repl-lag-hard-stop 200] \
    [--dirty-scale-start 0.05] \
    [--dirty-hard-stop 0.10] \
    [--dry-run]
```

## Arguments

| Argument | Required | Default | Description |
|---|---|---|---|
| `--uri` | yes | — | MongoDB connection URI |
| `--db` | yes | — | Database name |
| `--coll` | yes | — | Collection name |
| `--field` | yes | — | Date field used to determine document age |
| `--ttl` | yes | — | Retention period in seconds. Documents older than this are eligible for deletion. |
| `--max-batch` | no | `1000` | Maximum number of documents to delete per run. |
| `--interval` | no | `60` | Fixed sleep in seconds after each run. |
| `--repl-lag-hard-stop` | no | `200` | Replication lag in seconds that triggers a hard stop. |
| `--dirty-scale-start` | no | `0.10` | Dirty cache ratio (0–1) at which batch scaling begins. |
| `--dirty-hard-stop` | no | `0.20` | Dirty cache ratio (0–1) at which batch scaling reaches zero. |
| `--dry-run` | no | `false` | Log what would be deleted without actually deleting anything. |

## How it works

Each cycle the service:

1. Samples cluster health signals (see below)
2. If a hard stop is active, skips the run entirely
3. Computes an effective batch size based on current utilization
4. Fetches up to `batch_size` document IDs (oldest first, sorted by `--field`) and deletes them via `delete_many`
5. Sleeps for a computed interval after the delete completes

The sleep timer starts **after** the delete completes. Time spent on sampling and the delete itself does not count toward the interval — the cluster always gets the full `--interval` seconds of rest after work is done.

## Throttling signals

### Hard stops

Any of the following causes the entire run to be skipped:

| Signal | Source | Threshold |
|---|---|---|
| Replication lag | `replSetGetStatus` | > `--repl-lag-hard-stop` (default 200s) on any secondary |
| Write queue depth | `queues.execution.write.normalPriority.queueLength` | > 0 |

### Gradual scaling

When no hard stop is active, the batch size scales down linearly based on the dirty cache ratio:

| Signal | Source | Scale range |
|---|---|---|
| Dirty cache ratio | `wiredTiger.cache` dirty bytes / bytes in use | `--dirty-scale-start` → `--dirty-hard-stop` (default 10% → 20%) |

`batch_size = max_batch × (1 - score)`. At score 0 (below scale start) you get the full batch; at score 1 (at hard stop threshold) you get zero.

### Sleep interval

Fixed at `--interval` (default 60s) after every run. Only the batch size scales based on cluster health — the sleep is constant.

## Sharded clusters

On a sharded cluster the script connects via `mongos` for all delete operations. For health metric sampling, it connects **directly to each shard primary** using `directConnection=True`, which bypasses the router and allows `serverStatus` and `replSetGetStatus` to be run against the actual storage nodes.

This requires the MongoDB user to have the **`directShardOperations`** role in addition to the usual read/write permissions.

Only shards relevant to the delete query (resolved via `explain()`) are sampled each cycle. The worst value across all sampled shards governs throttling decisions.

## Notes

- An index on `--field` is strongly recommended. The batch ID fetch filters and sorts on this field every cycle.
- Writes use `w: majority` so deletions are durably acknowledged on a majority of nodes before the next cycle begins.
- The service is safe to restart at any time. It holds no local state — on restart it reconnects and picks up from whatever documents remain eligible.
