#!/usr/bin/env python3
"""
mongo_batch_deleter.py
----------------------
Long-lived TTL-style delete service for MongoDB.

Deletes expired documents in batches, backing off dynamically based on
cluster health signals sampled each cycle.

Hard stop signals (skip run entirely):
  - Replication lag exceeds 200s on any secondary
  - Write ticket utilization >= 90% or no tickets available
  - Write queue depth > 0

Gradual scaling signals (batch size scaled down linearly, take max):
  - Write ticket utilization: scale down from 70% toward 90%
  - Dirty cache ratio:        scale down from 55% toward 85%

Batch size is capped to the number of currently eligible documents.

Usage:
    python mongo_batch_deleter.py \
        --uri   "mongodb+srv://..." \
        --db    mydb \
        --coll  mycoll \
        --field createdAt \
        --ttl   31556952 \
        [--max-batch 1000] \
        [--dry-run]
"""

import argparse
import logging
import time
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient, WriteConcern

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("deleter")

# ── Constants ─────────────────────────────────────────────────────────────────
RUN_INTERVAL_MAX_SEC = 60  # sleep interval when cluster is under pressure
RUN_INTERVAL_MIN_SEC = 10  # sleep interval when cluster is very healthy

REPL_LAG_HARD_STOP_SEC = 200  # replication lag threshold for hard stop

TICKET_SCALE_START = 0.70  # begin scaling batch down at this ticket utilization
TICKET_HARD_STOP = 0.90  # hard stop at this ticket utilization

DIRTY_SCALE_START = 0.55  # begin scaling batch down at this dirty cache ratio
DIRTY_HARD_STOP = 0.85  # hard stop at this dirty cache ratio (soft — via scale)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _repl_lag_seconds(admin) -> float:
    """
    Returns the maximum replication lag in seconds across all secondaries,
    or 0.0 if the node is not part of a replica set.
    """
    try:
        status = admin.command("replSetGetStatus")
    except Exception:
        return 0.0

    primary_date = None
    member_dates = []
    for member in status.get("members", []):
        if member.get("stateStr") == "PRIMARY":
            primary_date = member.get("optimeDate")
        else:
            d = member.get("optimeDate")
            if d is not None:
                member_dates.append(d)

    if primary_date is None or not member_dates:
        return 0.0

    return max((primary_date - d).total_seconds() for d in member_dates)


def sample_cluster_utilization(admin, db) -> tuple[float, bool]:
    """
    Samples cluster health and returns (util, hard_stop) where:

      util       — 0.0–1.0 scaling score, the max of:
                     - Write ticket utilization: tickets_in_use / total_tickets
                     - Dirty cache ratio: dirty_bytes / max_cache_bytes
                   Governs linear batch size scaling.

      hard_stop  — True if any of the following are active:
                     - Replication lag > REPL_LAG_HARD_STOP_SEC on any secondary
                     - Write ticket utilization >= TICKET_HARD_STOP
                     - No write tickets available
                     - Write queue depth > 0
                   Causes the run to be skipped entirely.

    Signals logged per cycle:
      repl_lag, ticket_util, write_queue_depth, dirty_util
    """
    status = db.command("serverStatus")

    # ── Replication lag ───────────────────────────────────────────────────────
    repl_lag = _repl_lag_seconds(db.client.admin)

    # ── Write ticket utilization ──────────────────────────────────────────────
    wt = status["queues"]["execution"]["write"]
    tickets_out = wt["out"]
    tickets_total = wt["totalTickets"]
    tickets_avail = wt["available"]
    ticket_util = tickets_out / tickets_total if tickets_total > 0 else 1.0

    # ── Write queue depth ─────────────────────────────────────────────────────
    write_queue = wt["normalPriority"]["queueLength"]

    # ── Dirty cache ratio ─────────────────────────────────────────────────────
    cache = status["wiredTiger"]["cache"]
    dirty_util = (
        cache["tracked dirty bytes in the cache"] / cache["maximum bytes configured"]
    )

    log.info(
        "Repl lag: %.1fs  |  Tickets: %d/%d (%.1f%%)  |  Write queue: %d  |  Dirty cache: %.1f%%",
        repl_lag,
        tickets_out,
        tickets_total,
        ticket_util * 100,
        write_queue,
        dirty_util * 100,
    )

    hard_stop = (
        repl_lag > REPL_LAG_HARD_STOP_SEC
        or ticket_util >= TICKET_HARD_STOP
        or tickets_avail == 0
        or write_queue > 0
    )

    # Scaling util is the max of the two gradual signals, each normalized to
    # their own scale range so they are comparable on a 0–1 axis.
    def _normalize(val: float, start: float, stop: float) -> float:
        if val <= start:
            return 0.0
        if val >= stop:
            return 1.0
        return (val - start) / (stop - start)

    util = max(
        _normalize(ticket_util, TICKET_SCALE_START, TICKET_HARD_STOP),
        _normalize(dirty_util, DIRTY_SCALE_START, DIRTY_HARD_STOP),
    )

    return util, hard_stop


def compute_sleep_interval(util: float, hard_stop: bool) -> float:
    """
    Returns the post-work sleep interval for this cycle.

    When the cluster is healthy (util == 0.0, no hard stop), sleeps for
    RUN_INTERVAL_MIN_SEC so backlogs clear faster. Scales up linearly to
    RUN_INTERVAL_MAX_SEC as util approaches 1.0 or on any hard stop.
    """
    if hard_stop or util >= 1.0:
        return RUN_INTERVAL_MAX_SEC

    return RUN_INTERVAL_MIN_SEC + util * (RUN_INTERVAL_MAX_SEC - RUN_INTERVAL_MIN_SEC)


def compute_batch_size(max_batch: int, util: float) -> int:
    """
    Returns the effective batch size for this run.

    util is a 0–1 normalized score (0 = no pressure, 1 = at hard-stop
    threshold). Batch scales down linearly from max_batch to 0 as util
    goes from 0 to 1. Hard stops are handled before this is called.
    """
    return max(0, int(max_batch * (1.0 - util)))


def eligible_time_range(coll, field: str, cutoff: datetime):
    """
    Returns (min_time, max_time, total_count) for documents eligible for
    deletion. min/max are None if no documents qualify.
    """
    pipeline = [
        {"$match": {field: {"$lte": cutoff}}},
        {
            "$group": {
                "_id": None,
                "min_t": {"$min": f"${field}"},
                "max_t": {"$max": f"${field}"},
                "count": {"$sum": 1},
            }
        },
    ]
    result = list(coll.aggregate(pipeline))
    if not result:
        return None, None, 0
    r = result[0]
    return r["min_t"], r["max_t"], r["count"]


# ── Main service loop ─────────────────────────────────────────────────────────


def run(args: argparse.Namespace) -> None:
    client = MongoClient(args.uri)
    db = client[args.db]
    coll = db[args.coll].with_options(write_concern=WriteConcern(w="majority"))

    client.admin.command("ping")
    log.info("Connected to MongoDB.")

    while True:
        cycle_start = time.monotonic()

        # ── Sample cluster utilization ────────────────────────────────────────
        util, hard_stop = sample_cluster_utilization(client.admin, db)

        if hard_stop:
            log.warning("Hard stop signal active — skipping run.")
        else:
            # ── Determine eligibility window ──────────────────────────────────
            cutoff = datetime.fromtimestamp(
                datetime.now(tz=timezone.utc).timestamp() - args.ttl,
                tz=timezone.utc,
            )

            min_t, max_t, total_eligible = eligible_time_range(coll, args.field, cutoff)

            if total_eligible == 0:
                log.info("No expired documents found — sleeping.")
            else:
                log.info(
                    "Eligible: %d docs  |  oldest: %s  |  newest: %s",
                    total_eligible,
                    min_t,
                    max_t,
                )

                # ── Compute batch size ────────────────────────────────────────
                batch_size = compute_batch_size(args.max_batch, util)

                # Never attempt more docs than actually exist
                batch_size = min(batch_size, total_eligible)

                log.info("Effective batch: %d", batch_size)

                if batch_size == 0:
                    log.warning(
                        "utilization too high (%.1f%%) — skipping run.", util * 100
                    )
                else:
                    # ── Fetch eligible IDs then delete ────────────────────────
                    # Collect up to batch_size _ids sorted by the TTL field
                    # ascending (oldest first), then issue a single delete_many
                    # keyed on those exact IDs.
                    query = {args.field: {"$lte": cutoff}}
                    ids = [
                        doc["_id"]
                        for doc in coll.find(query, {"_id": 1})
                        .sort(args.field, ASCENDING)
                        .limit(batch_size)
                    ]

                    if args.dry_run:
                        deleted = len(ids)
                        log.info(
                            "[DRY-RUN] Would delete %d docs  |  oldest _id: %s  |  newest _id: %s",
                            deleted,
                            ids[0],
                            ids[-1],
                        )
                    else:
                        delete_start = time.monotonic()
                        result = coll.delete_many({"_id": {"$in": ids}})
                        deleted = result.deleted_count
                        log.info(
                            "Deleted %d docs in %.1fs  |  oldest _id: %s  |  newest _id: %s",
                            deleted,
                            time.monotonic() - delete_start,
                            ids[0],
                            ids[-1],
                        )

                    remaining = total_eligible - deleted
                    if remaining > 0:
                        log.info("%d docs remain — queued for next run.", remaining)

        # ── Sleep after work completes ────────────────────────────────────────
        interval = compute_sleep_interval(util, hard_stop)
        elapsed = time.monotonic() - cycle_start
        log.info(
            "Run took %.1fs — sleeping %.1fs (interval %.0fs).\n",
            elapsed,
            interval,
            interval,
        )
        time.sleep(interval)


# ── CLI ───────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="TTL-style MongoDB batch delete service.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--uri", required=True, help="MongoDB connection URI")
    p.add_argument("--db", required=True, help="Database name")
    p.add_argument("--coll", required=True, help="Collection name")
    p.add_argument("--field", required=True, help="Date field for TTL comparison")
    p.add_argument(
        "--ttl",
        required=True,
        type=int,
        help="Retention period in seconds (docs older than this are deleted)",
    )

    p.add_argument(
        "--max-batch",
        default=1_000,
        type=int,
        help="Upper bound on documents deleted per run",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without deleting",
    )
    return p


if __name__ == "__main__":
    run(build_parser().parse_args())
