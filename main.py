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

Gradual scaling signals (batch size reduced linearly, worst signal "wins"):
  - Replication lag: scales from 50% of repl-lag-hard-stop toward 100%
  - Dirty cache ratio: scales from dirty-scale-start toward dirty-hard-stop

Topology:
  - Replica set: metrics sampled directly from the primary via serverStatus
    and replSetGetStatus. No Atlas Admin API credentials required.
  - Sharded cluster (Atlas): metrics fetched from the Atlas Admin API using
    HTTP Digest auth (public key + private key). No direct shard connections
    are made; the directShardOperations role is not required.

Modes:
  Default  — reads config from CLI args, connects using --uri, runs one cycle,
             exits. Scheduling is left entirely to the caller (cron, systemd
             timer, etc.).

  --lambda — reads config from environment variables, fetches the MongoDB URI
             from AWS Secrets Manager, caches the MongoClient across warm
             invocations, and exposes a handler(event, context) entry point
             for AWS Lambda / EventBridge. The --lambda flag is only needed
             when invoking via CLI for local testing; Lambda itself calls
             handler() directly.

Usage (default):
    python main.py \\
        --uri   "mongodb+srv://..." \\
        --db    mydb \\
        --coll  mycoll \\
        --field createdAt \\
        --ttl   31556952 \\
        [--max-batch 1000] \\
        [--repl-lag-hard-stop 200] \\
        [--dirty-scale-start 0.10] \\
        [--dirty-hard-stop 0.20] \\
        [--atlas-group-id <id>] \\
        [--atlas-public-key <key>] \\
        [--atlas-private-key <key>] \\
        [--atlas-cluster-name <name>] \\
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
        ATLAS_CLUSTER_NAME  — Atlas cluster name

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

import httpx
from pymongo import ASCENDING, MongoClient, WriteConcern

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("deleter")

# ── Atlas Admin API base URL ──────────────────────────────────────────────────
_ATLAS_BASE = "https://cloud.mongodb.com/api/atlas/v2"

# ── Lambda: module-level cache (reused across warm invocations) ───────────────
_lambda_mongo_client: MongoClient | None = None
_lambda_mongo_uri: str | None = None


# ── Topology detection ────────────────────────────────────────────────────────


def _is_mongos(admin) -> bool:
    """
    Returns True if the connected node is a mongos (sharded cluster router).

    Raises on any error — a failed hello command means topology is unknown,
    and silently falling through to the replica-set path would produce
    incorrect results against a mongos.
    """
    result = admin.command("hello")
    return result.get("msg") == "isdbgrid"


# ── Lambda: Secrets Manager URI fetch ────────────────────────────────────────


def _get_lambda_uri() -> str:
    """
    Fetches the MongoDB URI from AWS Secrets Manager and caches it for the
    lifetime of the Lambda execution environment.
    """
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
    """
    Returns a cached MongoClient for Lambda. A new connection is only
    established on a cold start.
    """
    global _lambda_mongo_client
    if _lambda_mongo_client is None:
        _lambda_mongo_client = MongoClient(_get_lambda_uri())
        _lambda_mongo_client.admin.command("ping")
        log.info("MongoDB connection established (cold start).")
    return _lambda_mongo_client


# ── Atlas Admin API helpers ───────────────────────────────────────────────────


def _atlas_get(path: str, public_key: str, private_key: str, params) -> dict:
    """
    Issues an authenticated GET request to the Atlas Admin API v2 using
    HTTP Digest auth and returns the parsed JSON body.

    params may be a dict or a list of (key, value) tuples to allow repeated
    query parameters (e.g. multiple m= values for metric selection).

    Raises httpx.HTTPStatusError on non-2xx responses.
    """
    url = f"{_ATLAS_BASE}{path}"
    headers = {"Accept": "application/vnd.atlas.2023-01-01+json"}
    with httpx.Client() as client:
        response = client.get(
            url,
            params=params,
            headers=headers,
            auth=httpx.DigestAuth(public_key, private_key),
            timeout=15.0,
        )
    response.raise_for_status()
    return response.json()


def _latest_value(measurements: list, metric_name: str) -> float | None:
    """
    Returns the most recent non-null data point value for the named metric
    from an Atlas measurements array, or None if no data is available.
    """
    for m in measurements:
        if m.get("name") == metric_name:
            for dp in reversed(m.get("dataPoints", [])):
                if dp.get("value") is not None:
                    return float(dp["value"])
    return None


def _atlas_list_processes(group_id: str, public_key: str, private_key: str) -> list:
    """
    Returns all MongoDB processes registered in the Atlas project, fetching
    all pages.

    Each entry includes:
      id            — "{hostname}:{port}" used as processId in subsequent calls
      typeName      — e.g. REPLICA_PRIMARY, REPLICA_SECONDARY, SHARD_PRIMARY
      replicaSetName, shardName, ...
    """
    results = []
    
    # handle pagination
    page = 1
    page_size = 500
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


def _atlas_process_metrics(
    group_id: str,
    public_key: str,
    private_key: str,
    process_id: str,
    metrics: list[str],
    period: str = "PT2M",
    granularity: str = "PT1M",
) -> dict[str, float | None]:
    """
    Fetches the latest value for each requested process-level metric for a
    single Atlas process. Returns {metric_name: latest_value_or_None}.
    """
    params: list[tuple[str, str]] = [("granularity", granularity), ("period", period)]
    for m in metrics:
        params.append(("m", m))

    data = _atlas_get(
        f"/groups/{group_id}/processes/{process_id}/measurements",
        public_key,
        private_key,
        params=params,
    )
    measurements = data.get("measurements", [])
    return {m: _latest_value(measurements, m) for m in metrics}


def _atlas_list_disks(
    group_id: str, public_key: str, private_key: str, process_id: str
) -> list[str]:
    """Returns partition names for a process. The first is used for disk checks."""
    data = _atlas_get(
        f"/groups/{group_id}/processes/{process_id}/disks",
        public_key,
        private_key,
        params={"itemsPerPage": 50},
    )
    return [r["partitionName"] for r in data.get("results", [])]


def _atlas_node_metrics(
    group_id: str,
    public_key: str,
    private_key: str,
    process_id: str,
    type_name: str,
) -> dict:
    """
    Fetches health metrics for a single Atlas process via the Admin API.

    OPLOG_REPLICATION_LAG is only requested for non-PRIMARY nodes; the
    primary has no lag by definition. Returns:
      repl_lag, write_queue, dirty_util, disk_queue_hard_stop
    """
    is_primary = "PRIMARY" in type_name.upper()

    metrics_to_fetch = ["DIRTY_FILL_RATIO", "GLOBAL_LOCK_CURRENT_QUEUE_WRITERS"]
    if not is_primary:
        metrics_to_fetch.append("OPLOG_REPLICATION_LAG")

    proc_values = _atlas_process_metrics(
        group_id,
        public_key,
        private_key,
        process_id,
        metrics_to_fetch,
        period="PT2M",
        granularity="PT1M",
    )

    repl_lag = max(0.0, proc_values.get("OPLOG_REPLICATION_LAG") or 0.0)
    write_queue = proc_values.get("GLOBAL_LOCK_CURRENT_QUEUE_WRITERS") or 0.0
    dirty_util = (proc_values.get("DIRTY_FILL_RATIO") or 0.0) / 100.0

    # Disk queue depth hard stop: current value >= daily max.
    # A single P1D request covers both: the most recent non-null point is the
    # current value; max() over all points is the daily max.
    disk_queue_hard_stop = False
    try:
        partitions = _atlas_list_disks(group_id, public_key, private_key, process_id)
        if partitions:
            partition = partitions[0]
            disk_path = (
                f"/groups/{group_id}/processes/{process_id}"
                f"/disks/{partition}/measurements"
            )

            daily_data = _atlas_get(
                disk_path,
                public_key,
                private_key,
                params=[
                    ("granularity", "PT1M"),
                    ("period", "P1D"),
                    ("m", "DISK_QUEUE_DEPTH"),
                ],
            )
            daily_points = [
                dp["value"]
                for m in daily_data.get("measurements", [])
                if m.get("name") == "DISK_QUEUE_DEPTH"
                for dp in m.get("dataPoints", [])
                if dp.get("value") is not None
            ]

            if daily_points:
                current = daily_points[-1]  # most recent non-null point
                daily_max = max(daily_points)
                disk_queue_hard_stop = current >= daily_max
                log.info(
                    "[%s]  Disk queue depth: %.1f  |  Daily max: %.1f  |  Hard stop: %s",
                    process_id,
                    current,
                    daily_max,
                    disk_queue_hard_stop,
                )
    except Exception as e:
        log.warning(
            "[%s]  Could not fetch disk queue depth: %s. Skipping disk check.",
            process_id,
            e,
        )

    log.info(
        "[%s]  Replication lag: %.1fs  |  Write queue: %.0f  |  Dirty cache: %.1f%%",
        process_id,
        repl_lag,
        write_queue,
        dirty_util * 100,
    )

    return {
        "repl_lag": repl_lag,
        "write_queue": write_queue,
        "dirty_util": dirty_util,
        "disk_queue_hard_stop": disk_queue_hard_stop,
    }


# ── Replica-set health helpers ────────────────────────────────────────────────


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


# ── Explain helper ────────────────────────────────────────────────────────────


def _shards_for_pipeline(
    client: MongoClient, db_name: str, coll_name: str, pipeline: list
) -> set[str]:
    """
    Runs explain() on the aggregation pipeline against the mongos and returns
    the set of shard names the query will touch.

    Returns an empty set if the explain output cannot be parsed, which causes
    the caller to fall back to sampling all cluster processes.
    """
    try:
        explain = client[db_name].command(
            "explain",
            {"aggregate": coll_name, "pipeline": pipeline, "cursor": {}},
            verbosity="queryPlanner",
        )
        # The shards involved appear under splitPipeline.shardsPart or
        # shards keys at the top level of the explain output.
        shard_names = set()

        # mongos explains nest per-shard plans under "shards" at the top level.
        for key in explain.get("shards", {}):
            shard_names.add(key)

        # Older explain format: stages[0].$cursor.queryPlanner.winningPlan.shards
        for stage in explain.get("stages", []):
            cursor = stage.get("$cursor", {})
            for shard in (
                cursor.get("queryPlanner", {}).get("winningPlan", {}).get("shards", [])
            ):
                shard_names.add(shard.get("shardName", ""))

        shard_names.discard("")
        return shard_names
    except Exception as e:
        log.warning(
            "Could not run explain() to determine relevant shards: %s. "
            "Falling back to sampling all cluster processes.",
            e,
        )
        return set()


# ── Unified cluster utilization sampler ───────────────────────────────────────


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
    atlas_cluster_name: str | None = None,
) -> tuple[float, bool, float]:
    """
    Samples cluster health and returns (util, hard_stop, repl_lag).

    Topology detected automatically:
      - Replica set / standalone: serverStatus + replSetGetStatus on the primary.
      - Sharded cluster (mongos): Atlas Admin API. All four atlas_* args required.
        explain() is run on the delete pipeline to restrict sampling to only
        the shards the query actually touches.

    util       — 0.0–1.0 scaling factor; max of repl-lag and dirty-cache signals
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
        # ── Sharded cluster: Atlas Admin API ─────────────────────────────────
        missing = [
            name
            for name, val in [
                ("atlas_group_id", atlas_group_id),
                ("atlas_public_key", atlas_public_key),
                ("atlas_private_key", atlas_private_key),
                ("atlas_cluster_name", atlas_cluster_name),
            ]
            if not val
        ]
        if missing:
            raise ValueError(
                f"Sharded cluster detected but the following required Atlas API "
                f"arguments are missing: {', '.join(missing)}"
            )

        group_id_: str = atlas_group_id
        pub_key_: str = atlas_public_key
        priv_key_: str = atlas_private_key
        cluster_name_: str = atlas_cluster_name

        try:
            all_processes = _atlas_list_processes(group_id_, pub_key_, priv_key_)
        except Exception as e:
            log.warning("Could not list Atlas processes: %s — hard stop on safety.", e)
            return 1.0, True, 0.0

        # All processes belonging to this cluster.
        cluster_processes = [
            p
            for p in all_processes
            if cluster_name_ in (p.get("replicaSetName") or "")
            or cluster_name_ == (p.get("shardName") or "")
        ]

        if not cluster_processes:
            log.warning(
                "No processes found for cluster '%s' in group '%s'. "
                "Verify atlas_cluster_name and atlas_group_id. Hard stop on safety.",
                cluster_name_,
                group_id_,
            )
            return 1.0, True, 0.0

        # Restrict to shards the delete pipeline actually touches.
        relevant_shards = _shards_for_pipeline(client, db_name, coll_name, pipeline)
        if relevant_shards:
            cluster_processes = [
                p
                for p in cluster_processes
                if (p.get("shardName") or "") in relevant_shards
            ]
            log.info(
                "explain() identified %d relevant shard(s): %s. Sampling %d processes.",
                len(relevant_shards),
                sorted(relevant_shards),
                len(cluster_processes),
            )
        else:
            log.info(
                "Sampling all %d processes for cluster '%s'.",
                len(cluster_processes),
                cluster_name_,
            )

        all_metrics = []
        for proc in cluster_processes:
            process_id = proc["id"]
            type_name = proc.get("typeName", "")
            try:
                all_metrics.append(
                    _atlas_node_metrics(
                        group_id_, pub_key_, priv_key_, process_id, type_name
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
        disk_hard_stop = any(m["disk_queue_hard_stop"] for m in all_metrics)

        hard_stop = repl_lag >= repl_lag_hard_stop or write_queue > 0 or disk_hard_stop
        repl_lag_scale_start = repl_lag_hard_stop * 0.5
        util = max(
            _normalize(repl_lag, repl_lag_scale_start, repl_lag_hard_stop),
            _normalize(dirty_util, dirty_scale_start, dirty_hard_stop),
        )
        return util, hard_stop, repl_lag

    else:
        # ── Replica set / standalone: sample the primary directly ─────────────
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
        repl_lag_scale_start = repl_lag_hard_stop * 0.5
        util = max(
            _normalize(repl_lag, repl_lag_scale_start, repl_lag_hard_stop),
            _normalize(dirty_util, dirty_scale_start, dirty_hard_stop),
        )
        return util, hard_stop, repl_lag


def compute_batch_size(max_batch: int, util: float) -> int:
    """
    Returns the effective batch size for this run.

    util is a 0–1 score. Batch scales down linearly from max_batch to 0 as
    util goes from 0 to 1. Hard stops are handled before this is called.
    """
    return max(0, int(max_batch * (1.0 - util)))


# ── Core single-run logic (shared by both modes) ──────────────────────────────


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
    atlas_cluster_name: str | None,
    dry_run: bool,
) -> dict:
    """
    Executes one delete cycle and returns a result dict suitable for both
    direct logging and Lambda return values.

    Possible return values:
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

    # Build the delete pipeline once — used both for explain() and the actual fetch.
    pipeline = [
        {"$match": query},
        {"$sort": {field: ASCENDING}},
        {"$limit": max_batch},
        {"$project": {"_id": 1}},
    ]

    # ── Sample cluster health ─────────────────────────────────────────────────
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
        atlas_cluster_name=atlas_cluster_name,
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

    # ── Compute batch size ────────────────────────────────────────────────────
    batch_size = compute_batch_size(max_batch, util)

    if batch_size == 0:
        log.warning(
            "Cluster is under too much pressure (%.1f%% utilization). Skipping this run.",
            util * 100,
        )
        return {"status": "skipped", "reason": "utilization_too_high", "util": util}

    log.info("Deleting up to %d documents this run.", batch_size)

    pipeline = [
        {"$match": query},
        {"$sort": {field: ASCENDING}},
        {"$limit": batch_size},
        {"$project": {"_id": 1}},
    ]
    ids = [doc["_id"] for doc in coll.aggregate(pipeline)]

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
    deleted = result.deleted_count
    log.info(
        "Deleted %d documents in %.1fs. Oldest: %s  Newest: %s",
        deleted,
        time.monotonic() - delete_start,
        ids[0],
        ids[-1],
    )
    return {"status": "ok", "deleted": deleted, "util": round(util, 4)}


# ── Default mode ──────────────────────────────────────────────────────────────


def run_default(args: argparse.Namespace) -> None:
    """Connects using --uri from CLI args, runs one cycle, exits."""
    client = MongoClient(args.uri)
    client.admin.command("ping")
    log.info("Connected to MongoDB successfully.")

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
        atlas_cluster_name=args.atlas_cluster_name,
        dry_run=args.dry_run,
    )
    log.info("Result: %s", result)


# ── Lambda mode ───────────────────────────────────────────────────────────────


def handler(event, context) -> dict:
    """
    AWS Lambda entry point. Reads config from environment variables, fetches
    the MongoDB URI from Secrets Manager (cached after first call), and runs
    one delete cycle.

    Returns a structured result dict that is logged to CloudWatch.
    """
    client = _get_lambda_client()

    result = run_once(
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
        atlas_cluster_name=os.environ.get("ATLAS_CLUSTER_NAME"),
        dry_run=os.environ.get("MONGO_DRY_RUN", "").lower() == "true",
    )
    return result


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
        help=(
            "Run in Lambda mode: read config from environment variables, fetch "
            "URI from Secrets Manager, and invoke handler(). All other flags are "
            "ignored. Useful for local testing of the Lambda code path."
        ),
    )

    # Default-mode arguments — not required when --lambda is set.
    p.add_argument("--uri", default=None, help="MongoDB connection URI")
    p.add_argument("--db", default=None, help="Database name")
    p.add_argument("--coll", default=None, help="Collection name")
    p.add_argument("--field", default=None, help="Date field for TTL comparison")
    p.add_argument(
        "--ttl",
        default=None,
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
        "--atlas-group-id",
        default=None,
        help="Atlas project Group ID (24-hex string)",
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
    atlas.add_argument(
        "--atlas-cluster-name",
        default=None,
        help="Atlas cluster name (used to filter processes returned by the API)",
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
        # Invoke the Lambda handler directly for local testing.
        result = handler({}, None)
        log.info("Result: %s", result)
    else:
        # Validate that all default-mode required args are present.
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
