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
    --ttl       31556952 \
    [--max-batch 1000] \
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
| `--dry-run` | no | `false` | Log what would be deleted without actually deleting anything. |

## How it works

Each cycle the service:

1. Samples cluster health signals (see below)
2. If a hard stop is active, skips the run entirely
3. Runs an aggregation to count eligible documents and find their time range
4. Computes an effective batch size based on current utilization
5. Fetches up to `batch_size` document IDs (oldest first, sorted by `--field`) and deletes them via `delete_many`
6. Sleeps for a computed interval before the next cycle

The sleep timer starts **after** the delete completes. Time spent on sampling, aggregation, and the delete itself does not count toward the interval — the cluster always gets a full rest period after work is done.

## Throttling signals

### Hard stops

Any of the following causes the entire run to be skipped:

| Signal | Source | Threshold |
|---|---|---|
| Replication lag | `replSetGetStatus` | > 200s on any secondary |
| Write ticket utilization | `queues.execution.write` | ≥ 90% or no tickets available |
| Write queue depth | `queues.execution.write.normalPriority.queueLength` | > 0 |

### Gradual scaling

When no hard stop is active, the batch size scales down linearly based on the worse of two signals:

| Signal | Source | Scale range |
|---|---|---|
| Write ticket utilization | `queues.execution.write.out / totalTickets` | 70% → 90% |
| Dirty cache ratio | `wiredTiger.cache` dirty bytes / max bytes | 55% → 85% |

Each signal is normalized independently to a 0–1 score within its own range. The higher score drives the batch size: `batch_size = max_batch × (1 - score)`. At score 0 you get the full batch; at score 1 you get zero.

### Sleep interval

The post-work sleep scales between 10s (cluster healthy) and 60s (cluster under pressure or hard stop active), using the same combined utilization score.

## Notes

- An index on `--field` is strongly recommended. The eligibility aggregation and batch ID fetch both filter and sort on this field every cycle.
- Writes use `w: majority` so deletions are durably acknowledged on a majority of nodes before the next cycle begins.
- The service is safe to restart at any time. It holds no local state — on restart it reconnects and picks up from whatever documents remain eligible.
