"""
lambda.py
---------
AWS Lambda handler for TTL-style MongoDB batch deletes, triggered by an
Amazon EventBridge scheduled rule.

Each invocation performs a single batch delete and exits. EventBridge handles
the cadence. If cluster health signals indicate the cluster is under pressure,
the invocation exits early without deleting anything.

Mirrors the logic of main.py exactly. Metrics are sampled via direct shard
connections on sharded clusters (requires directShardOperations role).

Environment variables:
    Required:
        MONGO_SECRET_NAME   — Secrets Manager secret name containing {"uri": "..."}
        MONGO_DB            — Database name
        MONGO_COLLECTION    — Collection name
        MONGO_FIELD         — Date field used for TTL comparison
        MONGO_TTL_SECONDS   — Retention period in seconds

    Optional:
        MONGO_MAX_BATCH           — Max documents per invocation. Default 1000.
        MONGO_REPL_LAG_HARD_STOP  — Repl lag hard stop in seconds. Default 200.
        MONGO_DIRTY_SCALE_START   — Dirty cache ratio at which scaling begins. Default 0.10.
        MONGO_DIRTY_HARD_STOP     — Dirty cache ratio at which batch reaches zero. Default 0.20.

    Set automatically by the Lambda runtime:
        AWS_REGION
"""

import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import boto3
from pymongo import ASCENDING, MongoClient, WriteConcern

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("deleter")

# ── MongoDB client (reused across warm invocations) ───────────────────────────
_mongo_client: MongoClient | None = None
_mongo_uri: str | None = None


def _get_mongo_uri() -> str:
    """Fetch the MongoDB URI from AWS Secrets Manager (cached after first call)."""
    global _mongo_uri
    if _mongo_uri is None:
        secret_name = os.environ["MONGO_SECRET_NAME"]
        region = os.environ.get("AWS_REGION", "us-east-1")
        sm_client = boto3.client("secretsmanager", region_name=region)
        secret = sm_client.get_secret_value(SecretId=secret_name)
        _mongo_uri = json.loads(secret["SecretString"])["uri"]
    return _mongo_uri  # type: ignore[return-value]


def _get_client() -> MongoClient:
    """Return a cached MongoClient, creating one if this is a cold start."""
    global _mongo_client
    if _mongo_client is None:
        uri = _get_mongo_uri()
        _mongo_client = MongoClient(uri)
        _mongo_client.admin.command("ping")
        log.info("MongoDB connection established (cold start).")
    return _mongo_client


# ── Cluster health ────────────────────────────────────────────────────────────


def _is_mongos(admin) -> bool:
    """Returns True if the connected node is a mongos."""
    try:
        return admin.command("hello").get("msg") == "isdbgrid"
    except Exception:
        return False


def _repl_lag_seconds(admin) -> float:
    """Max replication lag in seconds across all secondaries, or 0.0."""
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

    return max(0.0, max((primary_date - d).total_seconds() for d in member_dates))


def _node_metrics(shard_id: str, admin) -> dict:
    """
    Samples serverStatus and replSetGetStatus from a single mongod.
    Returns: repl_lag, write_queue, dirty_util.
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


def _shard_admin_clients(
    client: MongoClient, uri: str, shard_ids: set | None = None
) -> list:
    """
    Returns a list of (shard_id, MongoClient) for each shard primary.
    Requires the directShardOperations role.
    """
    shards = client.admin.command("listShards").get("shards", [])
    parsed = urlparse(uri)
    qs = parse_qs(parsed.query)
    clients = []

    for shard in shards:
        if shard_ids is not None and shard["_id"] not in shard_ids:
            continue

        host = shard["host"]
        hosts = host.split("/", 1)[1] if "/" in host else host
        primary_host = hosts.split(",")[0]

        kwargs: dict = {
            "directConnection": True,
            "serverSelectionTimeoutMS": 5_000,
            "tls": True,
        }
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

        try:
            c = MongoClient(f"mongodb://{primary_host}/", **kwargs)
            c.admin.command("ping")
            clients.append((shard["_id"], c))
        except Exception as e:
            log.warning("Could not connect to shard %s: %s", shard["_id"], e)

    return clients


def _resolve_query_shards(coll, query: dict, field: str) -> set | None:
    """Returns the set of shard names the query routes to, or None."""
    try:
        pipeline = [
            {"$match": query},
            {"$sort": {field: ASCENDING}},
            {"$limit": 1},
            {"$project": {"_id": 1}},
        ]
        explained = coll.database.command(
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

        shards = _find_shards(explained)
        if shards:
            shard_ids = {s.get("shardName") or s.get("shard") for s in shards}
            shard_ids.discard(None)
            if shard_ids:
                log.info("Query routes to shards: %s", sorted(shard_ids))
                return shard_ids
    except Exception as e:
        log.warning("explain() failed: %s — will sample all shards.", e)
    return None


def _sample_cluster_utilization(
    client: MongoClient,
    uri: str,
    repl_lag_hard_stop: float,
    dirty_scale_start: float,
    dirty_hard_stop: float,
    shard_ids: set | None = None,
) -> tuple[float, bool]:
    """Returns (util, hard_stop) from cluster health signals."""

    def _normalize(val: float, start: float, stop: float) -> float:
        if val <= start:
            return 0.0
        if val >= stop:
            return 1.0
        return (val - start) / (stop - start)

    if _is_mongos(client.admin):
        shard_clients = _shard_admin_clients(client, uri, shard_ids=shard_ids)
        if not shard_clients:
            log.warning("No shard primaries reachable — skipping on safety.")
            return 1.0, True

        all_metrics = []
        for shard_id, sc in shard_clients:
            try:
                all_metrics.append(_node_metrics(shard_id, sc.admin))
            except Exception as e:
                log.warning("Failed to sample shard %s: %s — hard stop.", shard_id, e)
                return 1.0, True
            finally:
                sc.close()
    else:
        all_metrics = [_node_metrics("primary", client.admin)]

    repl_lag = max(m["repl_lag"] for m in all_metrics)
    write_queue = max(m["write_queue"] for m in all_metrics)
    dirty_util = max(m["dirty_util"] for m in all_metrics)

    hard_stop = repl_lag > repl_lag_hard_stop or write_queue > 0
    util = _normalize(dirty_util, dirty_scale_start, dirty_hard_stop)

    return util, hard_stop


# ── Lambda handler ────────────────────────────────────────────────────────────


def handler(event, context):
    """
    EventBridge scheduled rule handler.

    Returns a dict summarising what happened, logged to CloudWatch.
    """
    field = os.environ["MONGO_FIELD"]
    ttl = int(os.environ["MONGO_TTL_SECONDS"])
    max_batch = int(os.environ.get("MONGO_MAX_BATCH", "1000"))
    db_name = os.environ["MONGO_DB"]
    coll_name = os.environ["MONGO_COLLECTION"]

    repl_lag_hard_stop = float(os.environ.get("MONGO_REPL_LAG_HARD_STOP", "200"))
    dirty_scale_start = float(os.environ.get("MONGO_DIRTY_SCALE_START", "0.10"))
    dirty_hard_stop = float(os.environ.get("MONGO_DIRTY_HARD_STOP", "0.20"))

    uri = _get_mongo_uri()
    client = _get_client()
    coll = client[db_name][coll_name].with_options(
        write_concern=WriteConcern(w="majority")
    )

    # ── Determine eligibility window ──────────────────────────────────────────
    cutoff = datetime.fromtimestamp(
        datetime.now(tz=timezone.utc).timestamp() - ttl,
        tz=timezone.utc,
    )
    query = {field: {"$lte": cutoff}}
    shard_ids = (
        _resolve_query_shards(coll, query, field) if _is_mongos(client.admin) else None
    )

    # ── Sample cluster health ─────────────────────────────────────────────────
    util, hard_stop = _sample_cluster_utilization(
        client,
        uri,
        repl_lag_hard_stop=repl_lag_hard_stop,
        dirty_scale_start=dirty_scale_start,
        dirty_hard_stop=dirty_hard_stop,
        shard_ids=shard_ids,
    )

    if hard_stop:
        log.warning("Hard stop signal active — skipping run.")
        return {"status": "skipped", "reason": "hard_stop"}

    # ── Compute batch size ────────────────────────────────────────────────────
    batch_size = max(0, int(max_batch * (1.0 - util)))

    if batch_size == 0:
        log.warning("Utilisation too high (%.1f%%) — skipping run.", util * 100)
        return {"status": "skipped", "reason": "utilisation_too_high", "util": util}

    log.info("Effective batch: %d", batch_size)

    # ── Fetch eligible IDs then delete ────────────────────────────────────────
    pipeline = [
        {"$match": query},
        {"$sort": {field: ASCENDING}},
        {"$limit": batch_size},
        {"$project": {"_id": 1}},
    ]
    ids = [doc["_id"] for doc in coll.aggregate(pipeline)]

    if not ids:
        log.info("No expired documents found.")
        return {"status": "skipped", "reason": "nothing_eligible"}

    result = coll.delete_many({"_id": {"$in": ids}})
    deleted = result.deleted_count
    log.info(
        "Deleted %d docs  |  oldest _id: %s  |  newest _id: %s",
        deleted,
        ids[0],
        ids[-1],
    )

    return {
        "status": "ok",
        "deleted": deleted,
        "util": round(util, 4),
    }
