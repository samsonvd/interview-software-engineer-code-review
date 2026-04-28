import json
import time
import uuid
from typing import Dict, Any, Optional

import structlog

logger = structlog.get_logger()

# --- Fake dependencies (pretend these are real clients, and assume they are correctly implemented) ---


class DB:
    def __init__(self):
        self.log = logger.bind(component="db")

    async def get_project_config(self, project_id: str) -> Dict[str, Any]:
        # In reality, a DB query
        self.log.info("fetching project configuration", project_id=project_id)
        return {
            "project_id": project_id,
            "enabled": True,
            "api_key": "proj_secret",  # stored here for simplicity
            "context_wait_ms": 50,
            "inference_timeout_ms": 200,
        }

    async def save_fingerprint(
        self,
        request_id: str,
        project_id: str,
        fingerprint_id: str,
        metrics: dict[str, Any],
        context: dict[str, Any],
        created_at: int,
    ) -> None:
        """
        Saves fingerprint result for request_id.
        Can fail due to transient DB errors.
        """
        # For the exercise: assume this may raise
        self.log.info(
            "saving fingerprint",
            request_id=request_id,
            project_id=project_id,
            fingerprint_id=fingerprint_id,
            created_at=created_at,
        )
        # no-op


class Redis:
    store: Dict[str, str] = {}

    def __init__(self):
        self.log = logger.bind(component="redis")

    async def get(self, key: str) -> Optional[str]:
        self.log.debug("get", key=key)
        return self.store.get(key)


class InferenceService:
    def __init__(self):
        self.log = logger.bind(component="inference")

    async def fingerprint(
        self,
        project_id: str,
        metrics: dict[str, Any],
        context: dict[str, Any],
        timeout_ms: int,
    ) -> Dict[str, Any]:
        """
        Returns {"fingerprint_id": "..."} on success.
        Sometimes fails (timeout, 5xx).
        """
        # pretend success
        fp = "fp_" + project_id
        self.log.info("fingerprint_success", project_id=project_id, fingerprint_id=fp)
        return {"fingerprint_id": fp}


class DataQueueingService:
    def __init__(self):
        self.log = logger.bind(component="dqs")

    async def upload(self, project_id: str, upload_data: dict[str, Any]) -> None:
        """
        Pushes arbitray data attached to a project_id to downstream services which
        store and index it.
        This mechanism is backed by a queue. If the queue is unavailable this will raise an exception.
        """
        # no-op


db = DB()
redis = Redis()
inference = InferenceService()
dqs = DataQueueingService()


async def ingest_metrics(request_body: str) -> Dict[str, Any]:
    """
    Ingest metrics.

    request_body is a string containing JSON. It should have a `project_id` and a large blob of
    data under the `metrics` key.

    This function will be called by a webapp endpoint.

    The steps for metrics ingestion are:
    1. Fetch the project configuration to check if ingestion is enabled.
    2. Fetch additional context from Redis. This is generated via a separate process and *SHOULD*
       be available by the time this function is called.
    3. Compute the fingerprint using the fingerprinting service.
    4. Persist metrics and the fingerprint response to the database.
    5. Upload the data to a downstream data queueing service.
    6. Return a request_id to the caller.

    CRITICAL: The request_id must only be returned if the result was successfully saved to the DB.
    """

    request_id = str(uuid.uuid4())
    received_at = int(time.time())

    log = logger.bind(
        component="metrics_ingestion", request_id=request_id, received_at=received_at
    )
    log = logger.bind(
        component="metrics_ingestion", request_id=request_id, received_at=received_at
    )

    # Parse request
    body = json.loads(request_body)

    project_id = body["project_id"]
    metrics = body["metrics"]
    trace_id = body.get("trace_id")

    log = log.bind(project_id=project_id, trace_id=trace_id)

    log.debug("request received")

    # Load config
    cfg = await db.get_project_config(project_id)
    if not cfg["enabled"]:
        log.warning("project disabled")
        return {"status": 403, "error": "disabled"}

    # Fetch context from Redis (race: might not exist yet)
    ctx = {}
    if trace_id:
        key = f"ctx:{trace_id}"
        raw = redis.get(key)

        # wait for context
        if raw is None:
            log.info("waiting for missing context", wait_ms=cfg["context_wait_ms"])
            time.sleep(cfg["context_wait_ms"] / 1000.0)
            raw = redis.get(key)

        if raw:
            ctx = json.loads(raw)

    # Call inference service
    try:
        resp = await inference.fingerprint(
            project_id, metrics, ctx, timeout_ms=cfg["inference_timeout_ms"]
        )
        fingerprint_id = resp["fingerprint_id"]
    except Exception:
        log.error("failed to call inference service")
        return {"status": 200, "request_id": request_id, "error": "inference failed"}

    # Persist fingerprint (must succeed before returning request_id)
    try:
        await db.save_fingerprint(
            request_id,
            project_id,
            fingerprint_id,
            metrics=metrics,
            context=ctx,
            created_at=received_at,
        )
    except Exception:
        log.error("failed to save fingerprint")

    # Forward to downstream (no timeout/retry)
    payload = {
        "request_id": request_id,
        "received_at": received_at,
        "project_id": project_id,
        "trace_id": trace_id,
        "fingerprint_id": fingerprint_id,
        "metrics": metrics,
        "context": ctx,
    }

    await dqs.upload(project_id, payload)

    log.info("metrics ingestion complete")
    return {
        "status": 200,
        "project_id": project_id,
        "request_id": request_id,
        "fingerprint_id": fingerprint_id,
    }
