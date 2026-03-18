#!/usr/bin/env python3
"""
mongo_batch_deleter.py
----------------------
Long-lived TTL-style delete service for MongoDB.

Deletes expired documents in batches, backing off dynamically based on
cluster health signals sampled each cycle.

Hard stop signals (skip run entirely):
  - Replication lag exceeds 200s on any secondary
  - Write queue depth > 0

Gradual scaling signals (batch size scaled down linearly):
  - Dirty cache ratio: scale down from 10% toward 20%

Topology:
  - Replica set: metrics sampled directly from the primary via serverStatus
    and replSetGetStatus.
  - Sharded cluster: metrics sampled by connecting directly to each shard
    primary using the same URI credentials. Requires the user to have the
    directShardOperations role. Only shards relevant to the query are sampled
    (resolved via explain()). Worst value across shards governs throttling.

Batch size is capped to the number of currently eligible documents.

Usage:
    python mongo_batch_deleter.py \
        --uri   "mongodb+srv://..." \
        --db    mydb \
        --coll  mycoll \
        --field createdAt \
        --ttl   31556952 \
        [--max-batch 1000] \
        [--interval 60] \
        [--repl-lag-hard-stop 200] \
        [--dirty-scale-start 0.10] \
        [--dirty-hard-stop 0.20] \
        [--dry-run]
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from pymongo import ASCENDING, MongoClient, WriteConcern

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("deleter")

# ── Helpers ───────────────────────────────────────────────────────────────────


def _is_mongos(admin) -> bool:
    """Returns True if the connected node is a mongos (sharded cluster router)."""
    try:
        result = admin.command("hello")
        return result.get("msg") == "isdbgrid"
    except Exception:
        return False


def _shard_admin_clients(
    client: MongoClient, uri: str, shard_ids: set | None = None
) -> list:
    """
    Returns a list of (shard_id, MongoClient) for each shard primary in the
    cluster. Connects directly to each shard using directConnection=True, which
    requires the directShardOperations role.

    Credentials and TLS settings are taken from the original URI so that Atlas
    and other authenticated deployments work correctly.

    If shard_ids is provided, only connects to those shards.
    """
    shards = client.admin.command("listShards").get("shards", [])
    clients = []
    for shard in shards:
        if shard_ids is not None and shard["_id"] not in shard_ids:
            continue
        # host field is "replicaSetName/host1:port,host2:port,..."
        # With directConnection=True we target the first listed host directly.
        # Rebuild the URI swapping in the shard host to carry credentials/TLS.
        host = shard["host"]
        hosts = host.split("/", 1)[1] if "/" in host else host
        primary_host = hosts.split(",")[0]

        parsed = urlparse(uri)
        qs = parse_qs(parsed.query)

        # Build kwargs from the original URI so credentials and TLS carry over.
        # Always use mongodb:// for direct shard connections — mongodb+srv://
        # does DNS-based resolution and does not support direct host overrides.
        kwargs: dict = {"directConnection": True, "serverSelectionTimeoutMS": 5_000}
        if parsed.username:
            kwargs["username"] = parsed.username
        if parsed.password:
            kwargs["password"] = parsed.password
        auth_source = qs.get("authSource", [None])[0]
        if auth_source:
            kwargs["authSource"] = auth_source
        auth_mech = qs.get("authMechanism", [None])[0]
        if auth_mech:
            kwargs["authMechanism"] = auth_mech
        kwargs["tls"] = True

        try:
            c = MongoClient(f"mongodb://{primary_host}/", **kwargs)
            c.admin.command("ping")
            clients.append((shard["_id"], c))
        except Exception as e:
            log.warning("Could not connect to shard %s: %s", shard["_id"], e)
    return clients


def _resolve_query_shards(coll, query: dict, field: str) -> set | None:
    """
    Runs explain() on the aggregation pipeline against the collection and
    returns the set of shard names the query will be routed to.

    Returns None if explain fails or the collection is unsharded, in which
    case the caller falls back to sampling all shards.
    """
    try:
        pipeline = [
            {"$match": query},
            {"$sort": {field: ASCENDING}},
            {"$limit": 1},
            {"$project": {"_id": 1}},
        ]
        explained_doc = coll.database.command(
            "explain",
            {"aggregate": coll.name, "pipeline": pipeline, "cursor": {}},
            verbosity="queryPlanner",
        )

        def _find_shards(obj):
            if isinstance(obj, dict):
                if "shards" in obj and isinstance(obj["shards"], list):
                    return obj["shards"]
                for v in obj.values():
                    result = _find_shards(v)
                    if result:
                        return result
            elif isinstance(obj, list):
                for item in obj:
                    result = _find_shards(item)
                    if result:
                        return result
            return None

        shards = _find_shards(explained_doc)
        if shards:
            shard_ids = {s.get("shardName") or s.get("shard") for s in shards}
            shard_ids.discard(None)
            if shard_ids:
                log.info("Query routes to shards: %s", sorted(shard_ids))
                return shard_ids
    except Exception as e:
        log.warning("explain() failed: %s — will sample all shards.", e)
    return None


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
        elif member.get("stateStr") == "SECONDARY":
            d = member.get("optimeDate")
            if d is not None:
                member_dates.append(d)

    if primary_date is None or not member_dates:
        return 0.0

    # Clamp to zero — negative values indicate minor clock skew between nodes.
    return max(0.0, max((primary_date - d).total_seconds() for d in member_dates))


def _node_metrics(shard_id: str, admin) -> dict:
    """
    Samples serverStatus and replSetGetStatus from a single mongod and returns
    a dict with: repl_lag, write_queue, dirty_util.
    """
    status = admin.command("serverStatus")

    repl_lag = _repl_lag_seconds(admin)
    write_queue = status["queues"]["execution"]["write"]["normalPriority"][
        "queueLength"
    ]

    cache = status["wiredTiger"]["cache"]
    cache_used = cache["bytes currently in the cache"]
    dirty_util = (
        cache["tracked dirty bytes in the cache"] / cache_used
        if cache_used > 0
        else 0.0
    )

    log.info(
        "[%s]  Repl lag: %.1fs  |  Write queue: %d  |  Dirty cache: %.1f%%",
        shard_id,
        repl_lag,
        write_queue,
        dirty_util * 100,
    )

    return {
        "repl_lag": repl_lag,
        "write_queue": write_queue,
        "dirty_util": dirty_util,
    }


def sample_cluster_utilization(
    client: MongoClient,
    uri: str,
    repl_lag_hard_stop: float,
    dirty_scale_start: float,
    dirty_hard_stop: float,
    shard_ids: set | None = None,
) -> tuple[float, bool]:
    """
    Samples cluster health and returns (util, hard_stop).

    Topology is detected automatically:
      - Replica set / standalone: samples the primary via serverStatus and
        replSetGetStatus.
      - Sharded cluster (mongos): connects directly to each relevant shard
        primary (requires directShardOperations role) and samples the same
        metrics. Worst value across all sampled shards governs decisions.

      util       — 0.0–1.0 scaling score derived from dirty cache ratio.

      hard_stop  — True if on any sampled node:
                     - Replication lag > repl_lag_hard_stop
                     - Write queue depth > 0
    """

    def _normalize(val: float, start: float, stop: float) -> float:
        if val <= start:
            return 0.0
        if val >= stop:
            return 1.0
        return (val - start) / (stop - start)

    if _is_mongos(client.admin):
        # ── Sharded: connect directly to each relevant shard primary ──────────
        shard_clients = _shard_admin_clients(client, uri, shard_ids=shard_ids)
        if not shard_clients:
            log.warning("No shard primaries reachable — skipping on safety.")
            return 1.0, True

        all_metrics = []
        for shard_id, sc in shard_clients:
            try:
                all_metrics.append(_node_metrics(shard_id, sc.admin))
            except Exception as e:
                log.warning(
                    "Failed to sample shard %s: %s — treating as hard stop.",
                    shard_id,
                    e,
                )
                return 1.0, True
            finally:
                sc.close()
    else:
        # ── Replica set / standalone: sample the primary directly ─────────────
        all_metrics = [_node_metrics("primary", client.admin)]

    # Take the worst value across all sampled nodes
    repl_lag = max(m["repl_lag"] for m in all_metrics)
    write_queue = max(m["write_queue"] for m in all_metrics)
    dirty_util = max(m["dirty_util"] for m in all_metrics)

    hard_stop = repl_lag > repl_lag_hard_stop or write_queue > 0

    util = _normalize(dirty_util, dirty_scale_start, dirty_hard_stop)

    return util, hard_stop


def compute_batch_size(max_batch: int, util: float) -> int:
    """
    Returns the effective batch size for this run.

    util is a 0–1 normalized score (0 = no pressure, 1 = at hard-stop
    threshold). Batch scales down linearly from max_batch to 0 as util
    goes from 0 to 1. Hard stops are handled before this is called.
    """
    return max(0, int(max_batch * (1.0 - util)))


# ── Main service loop ─────────────────────────────────────────────────────────


def run(args: argparse.Namespace) -> None:
    client = MongoClient(args.uri)
    db = client[args.db]
    coll = db[args.coll].with_options(write_concern=WriteConcern(w="majority"))

    client.admin.command("ping")
    log.info("Connected to MongoDB.")

    while True:
        cycle_start = time.monotonic()

        # ── Determine eligibility window ──────────────────────────────────────
        cutoff = datetime.fromtimestamp(
            datetime.now(tz=timezone.utc).timestamp() - args.ttl,
            tz=timezone.utc,
        )

        # ── Resolve which shards the query touches (sharded clusters only) ────
        query = {args.field: {"$lte": cutoff}}
        shard_ids = (
            _resolve_query_shards(coll, query, args.field)
            if _is_mongos(client.admin)
            else None
        )

        # ── Sample cluster utilization ────────────────────────────────────────
        util, hard_stop = sample_cluster_utilization(
            client,
            uri=args.uri,
            repl_lag_hard_stop=args.repl_lag_hard_stop,
            dirty_scale_start=args.dirty_scale_start,
            dirty_hard_stop=args.dirty_hard_stop,
            shard_ids=shard_ids,
        )

        if hard_stop:
            log.warning("Hard stop signal active — skipping run.")
        else:
            # ── Compute batch size ────────────────────────────────────────────
            batch_size = compute_batch_size(args.max_batch, util)

            if batch_size == 0:
                log.warning("Utilisation too high (%.1f%%) — skipping run.", util * 100)
            else:
                log.info("Effective batch: %d", batch_size)

                # ── Fetch eligible IDs then delete ────────────────────────────
                # Aggregation pipeline: $match → $sort → $limit → $project.
                pipeline = [
                    {"$match": query},
                    {"$sort": {args.field: ASCENDING}},
                    {"$limit": batch_size},
                    {"$project": {"_id": 1}},
                ]
                ids = [doc["_id"] for doc in coll.aggregate(pipeline)]

                if not ids:
                    log.info("No expired documents found — sleeping.")
                elif args.dry_run:
                    log.info(
                        "[DRY-RUN] Would delete %d docs  |  oldest _id: %s  |  newest _id: %s",
                        len(ids),
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

        # ── Sleep after work completes ────────────────────────────────────────
        elapsed = time.monotonic() - cycle_start
        log.info("Run took %.1fs — sleeping %.0fs.\n", elapsed, args.interval)
        time.sleep(args.interval)


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
        "--interval",
        default=60,
        type=float,
        help="Fixed sleep interval in seconds after each run",
    )
    p.add_argument(
        "--repl-lag-hard-stop",
        default=200,
        type=float,
        help="Replication lag in seconds that triggers a hard stop",
    )
    p.add_argument(
        "--dirty-scale-start",
        default=0.10,
        type=float,
        help="Dirty cache ratio at which batch scaling begins (0–1)",
    )
    p.add_argument(
        "--dirty-hard-stop",
        default=0.20,
        type=float,
        help="Dirty cache ratio at which batch scaling reaches zero (0–1)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without deleting",
    )
    return p


if __name__ == "__main__":
    run(build_parser().parse_args())
