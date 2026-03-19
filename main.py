#!/usr/bin/env python3
"""
mongo_batch_deleter.py
----------------------
TTL-style batch delete service for MongoDB.

Deletes one batch of expired documents per invocation, throttling the batch
size based on real-time cluster health signals.

Hard stop signals (skip the delete entirely):
  - Replication lag >= repl-lag-hard-stop on any non-primary node
  - Write queue depth > 0
  - Current disk queue depth >= maximum disk queue depth over the past day
    (sharded Atlas clusters only)

Gradual scaling signals (batch size reduced linearly, worst signal wins):
  - Replication lag: scales from 50% of repl-lag-hard-stop toward 100%
  - Dirty cache ratio: scales from dirty-scale-start toward dirty-hard-stop

Topology:
  - Replica set: metrics sampled directly from the primary via serverStatus
    and replSetGetStatus. No Atlas Admin API credentials required.
  - Sharded cluster (Atlas): metrics fetched from the Atlas Admin API using
    HTTP Digest auth (public key + private key).

Modes:
  Default  — reads config from CLI args, connects using --uri, runs one cycle, exits.
  --lambda — reads config from environment variables, fetches the MongoDB URI
             from AWS Secrets Manager, caches the MongoClient across warm
             invocations, and exposes a handler(event, context) entry point
             for AWS Lambda / EventBridge.

Usage (default):
    python main.py \\
        --uri   "mongodb+srv://..." \\
        --db    mydb \\
        --coll  mycoll \\
        --field createdAt \\
        --ttl   31556952 \\
        [--interval 60] \\
        [--max-batch 1000] \\
        [--repl-lag-hard-stop 200] \\
        [--dirty-scale-start 0.10] \\
        [--dirty-hard-stop 0.20] \\
        [--atlas-group-id <id>] \\
        [--atlas-public-key <key>] \\
        [--atlas-private-key <key>] \\
        [--dry-run]

Usage (lambda — local test):
    python main.py --lambda

Atlas API credentials are required when the connected cluster is a sharded
Atlas cluster. They are ignored on replica sets.

Lambda environment variables:
    Required:
        MONGO_SECRET_NAME   — Secrets Manager secret name: {"uri": "..."}
        MONGO_DB            — Database name
        MONGO_COLLECTION    — Collection name
        MONGO_FIELD         — Date field used for TTL comparison
        MONGO_TTL_SECONDS   — Retention period in seconds

    Required for sharded Atlas clusters:
        ATLAS_GROUP_ID      — Atlas project Group ID (24-hex string)
        ATLAS_PUBLIC_KEY    — Atlas API public key
        ATLAS_PRIVATE_KEY   — Atlas API private key

    Optional:
        MONGO_MAX_BATCH           — Max documents per run. Default 1000.
        MONGO_REPL_LAG_HARD_STOP  — Repl lag hard stop in seconds. Default 200.
        MONGO_DIRTY_SCALE_START   — Dirty cache ratio at which scaling begins. Default 0.10.
        MONGO_DIRTY_HARD_STOP     — Dirty cache ratio at which batch reaches zero. Default 0.20.
        MONGO_DRY_RUN             — Set to "true" to skip actual deletes. Default false.
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx
from pymongo import ASCENDING, MongoClient, WriteConcern

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("deleter")
logging.getLogger("httpx").setLevel(logging.WARNING)

_ATLAS_BASE = "https://cloud.mongodb.com/api/atlas/v2"

_lambda_mongo_client: MongoClient | None = None
_lambda_mongo_uri: str | None = None


# ── Lambda helpers ────────────────────────────────────────────────────────────


def _get_lambda_uri() -> str:
    global _lambda_mongo_uri
    if _lambda_mongo_uri is None:
        import json
        import boto3

        secret_name = os.environ["MONGO_SECRET_NAME"]
        region = os.environ.get("AWS_REGION", "us-east-1")
        sm = boto3.client("secretsmanager", region_name=region)
        secret = sm.get_secret_value(SecretId=secret_name)
        _lambda_mongo_uri = json.loads(secret["SecretString"])["uri"]
    return _lambda_mongo_uri


def _get_lambda_client() -> MongoClient:
    global _lambda_mongo_client
    if _lambda_mongo_client is None:
        _lambda_mongo_client = MongoClient(_get_lambda_uri())
        _lambda_mongo_client.admin.command("ping")
        log.info("MongoDB connection established (cold start).")
    return _lambda_mongo_client


# ── Atlas Admin API ───────────────────────────────────────────────────────────


def _atlas_get(path: str, public_key: str, private_key: str, params) -> dict:
    """GET from the Atlas Admin API v2 with HTTP Digest auth.

    Path segments are percent-encoded individually so that special characters
    (e.g. the colon in "hostname:port") are not misinterpreted by the URL parser.
    """
    encoded_path = "/".join(quote(seg, safe="") for seg in path.split("/"))
    url = f"{_ATLAS_BASE}{encoded_path}"
    with httpx.Client() as client:
        response = client.get(
            url,
            params=params,
            headers={"Accept": "application/vnd.atlas.2025-03-12+json"},
            auth=httpx.DigestAuth(public_key, private_key),
            timeout=15.0,
        )
    if not response.is_success:
        log.warning(
            "Atlas API GET %s → %d: %s",
            response.url,
            response.status_code,
            response.text[:500],
        )
    response.raise_for_status()
    return response.json()


def _latest_value(measurements: list, metric_name: str) -> float | None:
    """Returns the most recent non-null data point for the named metric, or None."""
    for m in measurements:
        if m.get("name") == metric_name:
            for dp in reversed(m.get("dataPoints", [])):
                if dp.get("value") is not None:
                    return float(dp["value"])
    return None


def _atlas_list_processes(group_id: str, public_key: str, private_key: str) -> list:
    """Returns all MongoDB processes in the Atlas project, paginated."""
    results = []
    page, page_size = 1, 500
    while True:
        data = _atlas_get(
            f"/groups/{group_id}/processes",
            public_key,
            private_key,
            params={"itemsPerPage": page_size, "pageNum": page},
        )
        page_results = data.get("results", [])
        results.extend(page_results)
        if len(page_results) < page_size:
            break
        page += 1
    return results


def _atlas_node_metrics(
    group_id: str, public_key: str, private_key: str, process_id: str, type_name: str
) -> dict:
    """Fetches health metrics for a single Atlas process.

    Returns: repl_lag, write_queue, dirty_util, disk_queue_hard_stop
    """
    is_primary = "PRIMARY" in type_name.upper()
    metrics_to_fetch = ["DIRTY_FILL_RATIO", "GLOBAL_LOCK_CURRENT_QUEUE_WRITERS"]
    if not is_primary:
        metrics_to_fetch.append("OPLOG_REPLICATION_LAG")

    raw_data = _atlas_get(
        f"/groups/{group_id}/processes/{process_id}/measurements",
        public_key,
        private_key,
        params={"granularity": "PT1M", "period": "PT30M"},
    )
    measurements = raw_data.get("measurements", [])
    proc_values = {m: _latest_value(measurements, m) for m in metrics_to_fetch}

    repl_lag = proc_values.get("OPLOG_REPLICATION_LAG") or 0.0
    write_queue = proc_values.get("GLOBAL_LOCK_CURRENT_QUEUE_WRITERS") or 0.0
    dirty_util = (proc_values.get("DIRTY_FILL_RATIO") or 0.0) / 100.0

    # Disk queue hard stop: trigger if the current value equals the daily maximum.
    disk_queue_current = None
    disk_queue_daily_max = None
    disk_queue_hard_stop = False
    try:
        disks_data = _atlas_get(
            f"/groups/{group_id}/processes/{process_id}/disks",
            public_key,
            private_key,
            params={"itemsPerPage": 50},
        )
        partitions = [r["partitionName"] for r in disks_data.get("results", [])]
        if partitions:
            disk_data = _atlas_get(
                f"/groups/{group_id}/processes/{process_id}/disks/{partitions[0]}/measurements",
                public_key,
                private_key,
                params={"granularity": "PT1M", "period": "P1D"},
            )
            daily_points = [
                dp["value"]
                for m in disk_data.get("measurements", [])
                if m.get("name") == "DISK_QUEUE_DEPTH"
                for dp in m.get("dataPoints", [])
                if dp.get("value") is not None
            ]
            if daily_points:
                disk_queue_current = daily_points[-1]
                disk_queue_daily_max = max(daily_points)
                disk_queue_hard_stop = disk_queue_current >= disk_queue_daily_max
    except Exception as e:
        log.warning(
            "[%s]  Could not fetch disk queue depth: %s. Skipping disk check.",
            process_id,
            e,
        )

    return {
        "repl_lag": repl_lag,
        "write_queue": write_queue,
        "dirty_util": dirty_util,
        "disk_queue_current": disk_queue_current,
        "disk_queue_daily_max": disk_queue_daily_max,
        "disk_queue_hard_stop": disk_queue_hard_stop,
    }


# ── Replica-set helpers ───────────────────────────────────────────────────────


def _is_mongos(admin) -> bool:
    """Returns True if the connected node is a mongos router."""
    return admin.command("hello").get("msg") == "isdbgrid"


def _repl_lag_seconds(admin) -> float:
    """Returns the maximum replication lag in seconds across all secondaries, or 0.0."""
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


# ── Shard targeting via explain ───────────────────────────────────────────────


def _shards_for_pipeline(
    client: MongoClient, db_name: str, coll_name: str, pipeline: list
) -> set[str]:
    """Returns the shard names the pipeline will touch, or an empty set on failure.

    An empty set causes the caller to fall back to sampling all processes.
    """
    try:
        explain = client[db_name].command(
            "explain",
            {"aggregate": coll_name, "pipeline": pipeline, "cursor": {}},
            verbosity="queryPlanner",
        )
        shard_names = set()

        for key in explain.get("shards", {}):
            shard_names.add(key)

        for stage in explain.get("stages", []):
            cursor = stage.get("$cursor", {})
            for shard in (
                cursor.get("queryPlanner", {}).get("winningPlan", {}).get("shards", [])
            ):
                shard_names.add(shard.get("shardName", ""))

        shard_names.discard("")
        return shard_names
    except Exception as e:
        log.warning("explain() failed: %s. Falling back to sampling all processes.", e)
        return set()


# ── Cluster utilization sampler ───────────────────────────────────────────────


def sample_cluster_utilization(
    client: MongoClient,
    db_name: str,
    coll_name: str,
    pipeline: list,
    repl_lag_hard_stop: float,
    dirty_scale_start: float,
    dirty_hard_stop: float,
    atlas_group_id: str | None = None,
    atlas_public_key: str | None = None,
    atlas_private_key: str | None = None,
) -> tuple[float, bool, float]:
    """Returns (util, hard_stop, repl_lag).

    Topology is detected automatically:
      - Replica set: serverStatus + replSetGetStatus on the primary.
      - Sharded cluster: Atlas Admin API. atlas_group_id, atlas_public_key,
        and atlas_private_key are required.

    util       — 0.0–1.0 scaling factor; worst of repl-lag and dirty-cache signals
    hard_stop  — True if any hard-stop threshold is breached
    repl_lag   — Worst replication lag in seconds across all sampled nodes
    """

    def _normalize(val: float, start: float, stop: float) -> float:
        if val <= start:
            return 0.0
        if val >= stop:
            return 1.0
        return (val - start) / (stop - start)

    if _is_mongos(client.admin):
        missing = [
            name
            for name, val in [
                ("atlas_group_id", atlas_group_id),
                ("atlas_public_key", atlas_public_key),
                ("atlas_private_key", atlas_private_key),
            ]
            if not val
        ]
        if missing:
            raise ValueError(
                f"Sharded cluster detected but Atlas API arguments are missing: {', '.join(missing)}"
            )

        group_id_ = atlas_group_id
        pub_key_ = atlas_public_key
        priv_key_ = atlas_private_key

        try:
            all_processes = _atlas_list_processes(group_id_, pub_key_, priv_key_)
        except Exception as e:
            log.warning("Could not list Atlas processes: %s — hard stop on safety.", e)
            return 1.0, True, 0.0

        # Exclude routers; only sample data-bearing shard nodes.
        _EXCLUDED_TYPES = { "SHARD_MONGOS" }
        cluster_processes = [
            p for p in all_processes if p.get("typeName", "") not in _EXCLUDED_TYPES
        ]

        if not cluster_processes:
            log.warning(
                "No sampleable processes found in group '%s'. Hard stop on safety.",
                group_id_,
            )
            return 1.0, True, 0.0

        # Restrict to shards the pipeline actually touches.
        # Intersect with known shard names so phantom explain entries (e.g. 'config'
        # on clusters where the config shard holds no user data) are ignored. On
        # clusters with an embedded config shard (typeName=SHARD_PRIMARY/SECONDARY,
        # shardName='config'), 'config' will be in known_shards and is retained.
        known_shards = {p["shardName"] for p in cluster_processes if p.get("shardName")}
        relevant_shards = (
            _shards_for_pipeline(client, db_name, coll_name, pipeline) & known_shards
        )
        if relevant_shards:
            cluster_processes = [
                p
                for p in cluster_processes
                if (p.get("shardName") or "") in relevant_shards
            ]
            log.info(
                "Sampling %d process(es) across shard(s): %s.",
                len(cluster_processes),
                sorted(relevant_shards),
            )
        else:
            log.info(
                "Sampling all %d data-bearing process(es).", len(cluster_processes)
            )

        all_metrics = []
        for proc in cluster_processes:
            process_id = proc["id"]
            try:
                all_metrics.append(
                    _atlas_node_metrics(
                        group_id_,
                        pub_key_,
                        priv_key_,
                        process_id,
                        proc.get("typeName", ""),
                    )
                )
            except Exception as e:
                log.warning(
                    "Could not fetch metrics for process %s: %s — hard stop on safety.",
                    process_id,
                    e,
                )
                return 1.0, True, 0.0

        repl_lag = max(m["repl_lag"] for m in all_metrics)
        write_queue = max(m["write_queue"] for m in all_metrics)
        dirty_util = max(m["dirty_util"] for m in all_metrics)
        disk_queue_current = max(
            (
                m["disk_queue_current"]
                for m in all_metrics
                if m["disk_queue_current"] is not None
            ),
            default=None,
        )
        disk_queue_daily_max = max(
            (
                m["disk_queue_daily_max"]
                for m in all_metrics
                if m["disk_queue_daily_max"] is not None
            ),
            default=None,
        )
        disk_hard_stop = any(m["disk_queue_hard_stop"] for m in all_metrics)

        if disk_queue_current is not None and disk_queue_daily_max is not None:
            log.info(
                "[cluster]  Replication lag: %.1fs  |  Write queue: %.0f  |  Dirty cache: %.1f%%  |  Disk queue: %.1f  |  Disk daily max: %.1f",
                repl_lag,
                write_queue,
                dirty_util * 100,
                disk_queue_current,
                disk_queue_daily_max,
            )
        else:
            log.info(
                "[cluster]  Replication lag: %.1fs  |  Write queue: %.0f  |  Dirty cache: %.1f%%",
                repl_lag,
                write_queue,
                dirty_util * 100,
            )

        hard_stop = repl_lag >= repl_lag_hard_stop or write_queue > 0 or disk_hard_stop
        util = max(
            _normalize(repl_lag, repl_lag_hard_stop * 0.5, repl_lag_hard_stop),
            _normalize(dirty_util, dirty_scale_start, dirty_hard_stop),
        )
        return util, hard_stop, repl_lag

    else:
        status = client.admin.command("serverStatus")
        repl_lag = _repl_lag_seconds(client.admin)
        write_queue = status["queues"]["execution"]["write"]["normalPriority"]["queueLength"]

        cache = status["wiredTiger"]["cache"]
        cache_used = cache["bytes currently in the cache"]
        dirty_util = (
            cache["tracked dirty bytes in the cache"] / cache_used
            if cache_used > 0
            else 0.0
        )

        log.info(
            "[primary]  Replication lag: %.1fs  |  Write queue: %d  |  Dirty cache: %.1f%%",
            repl_lag,
            write_queue,
            dirty_util * 100,
        )

        hard_stop = repl_lag >= repl_lag_hard_stop or write_queue > 0
        util = max(
            _normalize(repl_lag, repl_lag_hard_stop * 0.5, repl_lag_hard_stop),
            _normalize(dirty_util, dirty_scale_start, dirty_hard_stop),
        )
        return util, hard_stop, repl_lag


def compute_batch_size(max_batch: int, util: float) -> int:
    """Scales batch size linearly from max_batch down to 0 as util goes from 0 to 1."""
    return max(0, int(max_batch * (1.0 - util)))


# ── Core delete cycle ─────────────────────────────────────────────────────────


def run_once(
    client: MongoClient,
    db_name: str,
    coll_name: str,
    field: str,
    ttl: int,
    max_batch: int,
    repl_lag_hard_stop: float,
    dirty_scale_start: float,
    dirty_hard_stop: float,
    atlas_group_id: str | None,
    atlas_public_key: str | None,
    atlas_private_key: str | None,
    dry_run: bool,
) -> dict:
    """Executes one delete cycle. Return values:
    {"status": "skipped", "reason": "hard_stop"}
    {"status": "skipped", "reason": "utilization_too_high", "util": <float>}
    {"status": "skipped", "reason": "nothing_eligible"}
    {"status": "ok",      "deleted": <int>, "util": <float>}
    {"status": "dry_run", "would_delete": <int>, "util": <float>}
    """
    coll = client[db_name][coll_name].with_options(
        write_concern=WriteConcern(w="majority")
    )
    cutoff = datetime.now(tz=timezone.utc) - timedelta(seconds=ttl)
    query = {field: {"$lte": cutoff}}
    pipeline = [
        {"$match": query},
        {"$sort": {field: ASCENDING}},
        {"$limit": max_batch},
        {"$project": {"_id": 1}},
    ]

    util, hard_stop, repl_lag = sample_cluster_utilization(
        client,
        db_name=db_name,
        coll_name=coll_name,
        pipeline=pipeline,
        repl_lag_hard_stop=repl_lag_hard_stop,
        dirty_scale_start=dirty_scale_start,
        dirty_hard_stop=dirty_hard_stop,
        atlas_group_id=atlas_group_id,
        atlas_public_key=atlas_public_key,
        atlas_private_key=atlas_private_key,
    )

    if hard_stop:
        if repl_lag >= repl_lag_hard_stop:
            log.warning(
                "Secondaries are %.0fs behind (hard stop at %.0fs). Skipping this run.",
                repl_lag,
                repl_lag_hard_stop,
            )
        else:
            log.warning("Cluster health hard stop active. Skipping this run.")
        return {"status": "skipped", "reason": "hard_stop"}

    batch_size = compute_batch_size(max_batch, util)
    if batch_size == 0:
        log.warning("Cluster utilization at %.1f%%. Skipping this run.", util * 100)
        return {"status": "skipped", "reason": "utilization_too_high", "util": util}

    log.info("Batch size: %d / %d (util: %.1f%%)", batch_size, max_batch, util * 100)

    ids = [
        doc["_id"]
        for doc in coll.aggregate(
            [
                {"$match": query},
                {"$sort": {field: ASCENDING}},
                {"$limit": batch_size},
                {"$project": {"_id": 1}},
            ]
        )
    ]
    if not ids:
        log.info("No documents found older than the TTL cutoff.")
        return {"status": "skipped", "reason": "nothing_eligible"}

    if dry_run:
        log.info(
            "[DRY RUN] Would delete %d documents. Oldest: %s  Newest: %s",
            len(ids),
            ids[0],
            ids[-1],
        )
        return {"status": "dry_run", "would_delete": len(ids), "util": round(util, 4)}

    delete_start = time.monotonic()
    result = coll.delete_many({"_id": {"$in": ids}})
    log.info(
        "Deleted %d documents in %.1fs. Oldest: %s  Newest: %s",
        result.deleted_count,
        time.monotonic() - delete_start,
        ids[0],
        ids[-1],
    )
    return {"status": "ok", "deleted": result.deleted_count, "util": round(util, 4)}


# ── Entry points ──────────────────────────────────────────────────────────────


def run_default(args: argparse.Namespace) -> None:
    client = MongoClient(args.uri)
    client.admin.command("ping")
    log.info("Connected to MongoDB successfully.")

    while True:
        result = run_once(
            client=client,
            db_name=args.db,
            coll_name=args.coll,
            field=args.field,
            ttl=args.ttl,
            max_batch=args.max_batch,
            repl_lag_hard_stop=args.repl_lag_hard_stop,
            dirty_scale_start=args.dirty_scale_start,
            dirty_hard_stop=args.dirty_hard_stop,
            atlas_group_id=args.atlas_group_id,
            atlas_public_key=args.atlas_public_key,
            atlas_private_key=args.atlas_private_key,
            dry_run=args.dry_run,
        )
        log.info("Result: %s  |  Sleeping for %ds.", result, args.interval)
        print()
        time.sleep(args.interval)


def handler(event, context) -> dict:
    """AWS Lambda entry point."""
    client = _get_lambda_client()
    return run_once(
        client=client,
        db_name=os.environ["MONGO_DB"],
        coll_name=os.environ["MONGO_COLLECTION"],
        field=os.environ["MONGO_FIELD"],
        ttl=int(os.environ["MONGO_TTL_SECONDS"]),
        max_batch=int(os.environ.get("MONGO_MAX_BATCH", "1000")),
        repl_lag_hard_stop=float(os.environ.get("MONGO_REPL_LAG_HARD_STOP", "200")),
        dirty_scale_start=float(os.environ.get("MONGO_DIRTY_SCALE_START", "0.10")),
        dirty_hard_stop=float(os.environ.get("MONGO_DIRTY_HARD_STOP", "0.20")),
        atlas_group_id=os.environ.get("ATLAS_GROUP_ID"),
        atlas_public_key=os.environ.get("ATLAS_PUBLIC_KEY"),
        atlas_private_key=os.environ.get("ATLAS_PRIVATE_KEY"),
        dry_run=os.environ.get("MONGO_DRY_RUN", "").lower() == "true",
    )


# ── CLI ───────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="TTL-style MongoDB batch delete service.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--lambda",
        dest="lambda_mode",
        action="store_true",
        help="Run in Lambda mode: read config from environment variables and invoke handler(). Useful for local testing.",
    )
    p.add_argument("--uri", default=None, help="MongoDB connection URI")
    p.add_argument("--db", default=None, help="Database name")
    p.add_argument("--coll", default=None, help="Collection name")
    p.add_argument("--field", default=None, help="Date field for TTL comparison")
    p.add_argument("--ttl", default=None, type=int, help="Retention period in seconds")
    p.add_argument(
        "--interval",
        default=60,
        type=int,
        help="Seconds to sleep between runs (default: 60)",
    )
    p.add_argument(
        "--max-batch",
        default=1_000,
        type=int,
        help="Upper bound on documents deleted per run",
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

    atlas = p.add_argument_group("Atlas API (required for sharded Atlas clusters only)")
    atlas.add_argument(
        "--atlas-group-id", default=None, help="Atlas project Group ID (24-hex string)"
    )
    atlas.add_argument(
        "--atlas-public-key",
        default=None,
        help="Atlas API public key for HTTP Digest authentication",
    )
    atlas.add_argument(
        "--atlas-private-key",
        default=None,
        help="Atlas API private key for HTTP Digest authentication",
    )

    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without deleting",
    )
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()

    if args.lambda_mode:
        result = handler({}, None)
        log.info("Result: %s", result)
    else:
        missing = [
            flag
            for flag, val in [
                ("--uri", args.uri),
                ("--db", args.db),
                ("--coll", args.coll),
                ("--field", args.field),
                ("--ttl", args.ttl),
            ]
            if val is None
        ]
        if missing:
            build_parser().error(
                f"the following arguments are required: {', '.join(missing)}"
            )
        run_default(args)
