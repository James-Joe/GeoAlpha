"""
pipeline.py

Main Prefect orchestration flow for the GeoAlpha weekly ingestion pipeline.
Wraps the three ingestion scripts as parallel Prefect tasks and writes a
consolidated summary to the pipeline_runs MongoDB collection once all tasks
have completed.

Deployment entrypoint: pipeline.py:geoalpha_weekly_ingestion
Schedule: 06:00 UTC every Monday (configured in Prefect Cloud)

Credentials:
    MONGODB_URI is injected at runtime via a Prefect Secret block — it is
    NOT loaded from a local .env file in this script.
"""

import os
from datetime import datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.futures import wait
from pymongo import MongoClient

# Import the named entrypoint from each ingestion module.
# Each function handles its own error recovery and writes a per-task record
# to pipeline_runs; this flow writes an additional consolidated record.
from ingest_gdelt import ingest_gdelt
from ingest_rss import ingest_rss
from ingest_shipping import ingest_shipping


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=30)
def run_gdelt_task() -> dict:
    """Ingest GDELT geopolitical news articles."""
    logger = get_run_logger()
    logger.info("Starting GDELT ingestion")
    result = ingest_gdelt()
    logger.info(
        f"GDELT ingestion complete — "
        f"{result['inserted']} articles inserted, "
        f"{len(result['errors'])} errors, status: {result['status']}"
    )
    return result


@task(retries=2, retry_delay_seconds=30)
def run_rss_task() -> dict:
    """Ingest RSS feed articles."""
    logger = get_run_logger()
    logger.info("Starting RSS ingestion")
    result = ingest_rss()
    logger.info(
        f"RSS ingestion complete — "
        f"{result['inserted']} articles inserted, "
        f"{len(result['errors'])} errors, status: {result['status']}"
    )
    return result


@task(retries=2, retry_delay_seconds=30)
def run_shipping_task() -> dict:
    """Ingest yfinance shipping index signals."""
    logger = get_run_logger()
    logger.info("Starting shipping signals ingestion")
    result = ingest_shipping()
    logger.info(
        f"Shipping ingestion complete — "
        f"{result['inserted']} signals inserted, "
        f"{len(result['errors'])} errors, status: {result['status']}"
    )
    return result


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="geoalpha_weekly_ingestion")
def geoalpha_weekly_ingestion() -> None:
    """
    Weekly GeoAlpha ingestion flow.

    Submits all three ingestion tasks in parallel (they share no dependencies),
    waits for all to finish, then writes a consolidated summary record to the
    pipeline_runs MongoDB collection.

    Overall status logic:
        success — all tasks completed with no errors
        partial — at least one task succeeded but at least one failed or had errors
        failed  — every task failed
    """
    logger = get_run_logger()

    # Use a single run_id for the flow-level consolidated record
    flow_run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    started_at  = datetime.now(timezone.utc)

    logger.info(f"GeoAlpha weekly ingestion starting — flow_run_id: {flow_run_id}")

    # Submit all three tasks immediately so they run in parallel
    task_futures = {
        "gdelt":    run_gdelt_task.submit(),
        "rss":      run_rss_task.submit(),
        "shipping": run_shipping_task.submit(),
    }

    # Block until every task has finished (succeeded, failed, or been retried)
    wait(list(task_futures.values()))

    # Collect results — tasks that exhausted retries are recorded as failures
    task_results: dict[str, dict] = {}
    all_failed = True   # flipped to False as soon as any task succeeds
    any_failed = False  # flipped to True  as soon as any task fails

    for name, future in task_futures.items():
        try:
            result = future.result()
            task_results[name] = result
            # A task that returned data (even partially) counts as not fully failed
            if result.get("status") != "failed":
                all_failed = False
            if result.get("status") in ("failed", "partial"):
                any_failed = True
        except Exception as exc:
            # Task raised after exhausting retries
            task_results[name] = {
                "task":     name,
                "status":   "failed",
                "inserted": 0,
                "errors":   [str(exc)],
            }
            any_failed = True
            logger.error(f"Task '{name}' failed after retries: {exc}")

    # Derive overall flow status
    if all_failed:
        overall_status = "failed"
    elif any_failed:
        overall_status = "partial"
    else:
        overall_status = "success"

    completed_at    = datetime.now(timezone.utc)
    total_inserted  = sum(r.get("inserted", 0) for r in task_results.values())

    logger.info(
        f"All tasks complete — overall status: {overall_status}, "
        f"total documents inserted: {total_inserted}. "
        f"Writing consolidated summary to pipeline_runs."
    )

    # Write the consolidated flow-level summary to MongoDB.
    # MONGODB_URI is injected by the Prefect Secret block at runtime.
    mongodb_uri = os.environ.get("MONGODB_URI")
    if not mongodb_uri:
        logger.error(
            "MONGODB_URI not set — cannot write consolidated pipeline_run record. "
            "Ensure the Prefect Secret block is configured correctly."
        )
        return

    client = MongoClient(mongodb_uri)
    try:
        runs_col = client["geoalpha"]["pipeline_runs"]
        run_record = {
            "run_id":          flow_run_id,
            "run_type":        "orchestrated_flow",   # distinguishes from per-task records
            "started_at":      started_at,
            "completed_at":    completed_at,
            "status":          overall_status,
            "task_results":    task_results,           # per-task inserted counts and errors
            "total_inserted":  total_inserted,
        }
        runs_col.insert_one(run_record)
        logger.info(f"Consolidated run record written — run_id: {flow_run_id}")
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Local runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Allows the flow to be tested locally: python pipeline.py
    # Ensure MONGODB_URI is set in your .env or shell environment before running.
    geoalpha_weekly_ingestion()
