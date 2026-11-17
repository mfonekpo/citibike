import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from threading import Lock
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, StringConstraints

from rag_system.ingestion.embed import upload_vector_to_qdrant
from rag_system.retrieval.search import ask_citibike_rag


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEDULE_TIMEZONE = timezone.utc
MAX_JOB_HISTORY = 100


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class QuestionRequest(BaseModel):
    question: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=3,
            max_length=1000,
        ),
    ] = Field(
        examples=[
            "Which month had the most Citibike trips?",
            "tell me the recipe of chocolate pancake",
        ]
    )


class QuestionResponse(BaseModel):
    question: str
    understood_query: str
    answer: str
    sources: list[dict[str, Any]]


class EmbeddingJob(BaseModel):
    job_id: str
    trigger: Literal["manual", "scheduled"]
    status: Literal["queued", "running", "completed", "failed"]
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: dict[str, Any] | None = None
    error: str | None = None


class EmbeddingAccepted(BaseModel):
    status: Literal["accepted"] = "accepted"
    job_id: str
    status_url: str
    triggered_at: datetime


class EmbeddingJobManager:
    """Coordinate refreshes and keep the latest 100 jobs in this process."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._jobs: dict[str, EmbeddingJob] = {}
        self._active_job_id: str | None = None

    def reserve(
        self,
        trigger: Literal["manual", "scheduled"],
    ) -> EmbeddingJob | None:
        # Reserve before scheduling work, so two callers cannot both start.
        with self._lock:
            if self._active_job_id is not None:
                return None

            job = EmbeddingJob(
                job_id=str(uuid4()),
                trigger=trigger,
                status="queued",
                created_at=utc_now(),
            )

            self._jobs[job.job_id] = job
            self._active_job_id = job.job_id

            while len(self._jobs) > MAX_JOB_HISTORY:
                oldest_id = next(iter(self._jobs))
                del self._jobs[oldest_id]

            return job.model_copy(deep=True)

    def get(self, job_id: str) -> EmbeddingJob | None:
        with self._lock:
            job = self._jobs.get(job_id)

            return job.model_copy(deep=True) if job else None

    def summary(self) -> dict[str, Any]:
        with self._lock:
            latest_id = next(reversed(self._jobs), None)
            latest = self._jobs[latest_id] if latest_id else None

            return {
                "active_job_id": self._active_job_id,
                "latest_job": (
                    latest.model_dump(mode="json")
                    if latest
                    else None
                ),
            }

    def _finish(
        self,
        job_id: str,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            if self._active_job_id != job_id:
                raise RuntimeError(
                    "Job does not own the active reservation."
                )

            job = self._jobs[job_id]
            job.status = "failed" if error else "completed"
            job.result = result
            job.error = error
            job.finished_at = utc_now()

            self._active_job_id = None

    def fail_to_start(self, job_id: str) -> None:
        self._finish(
            job_id,
            error=(
                "Could not start the refresh. "
                "Check the API server logs."
            ),
        )

    def run(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs[job_id]

            if (
                self._active_job_id != job_id
                or job.status != "queued"
            ):
                raise RuntimeError(
                    "Job is not queued for execution."
                )

            job.status = "running"
            job.started_at = utc_now()

        logger.info(
            "Embedding job %s started.",
            job_id,
        )

        # Network calls run outside the state lock,
        # allowing status requests while ingestion runs.
        try:
            result = upload_vector_to_qdrant()

            if (
                not isinstance(result, dict)
                or result.get("status") != "completed"
            ):
                raise RuntimeError(
                    "Ingestion did not return a completed "
                    "refresh summary."
                )

        except Exception:
            logger.exception(
                "Embedding job %s failed.",
                job_id,
            )

            self._finish(
                job_id,
                error=(
                    "Embedding refresh failed. "
                    "Check the API server logs."
                ),
            )

            return False

        self._finish(
            job_id,
            result=result,
        )

        logger.info(
            "Embedding job %s completed.",
            job_id,
        )

        return True

    def run_scheduled(self) -> None:
        job = self.reserve("scheduled")

        if job is None:
            logger.info(
                "Scheduled refresh skipped: "
                "another refresh is active."
            )
            return

        if not self.run(job.job_id):
            # Also report failure to APScheduler.
            raise RuntimeError(
                f"Scheduled embedding job failed: {job.job_id}"
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = EmbeddingJobManager()

    scheduler = BackgroundScheduler(
        timezone=SCHEDULE_TIMEZONE,
    )

    app.state.embedding_jobs = manager
    app.state.scheduler = scheduler

    scheduler.add_job(
        manager.run_scheduled,
        trigger="cron",
        hour="0,12",
        minute=0,
        id="twice_daily_embedding_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )

    scheduler.start()

    logger.info(
        "Embedding scheduler started: "
        "00:00 and 12:00 UTC."
    )

    try:
        yield

    finally:
        # Wait for scheduled work without blocking
        # the async event loop.
        await asyncio.to_thread(
            scheduler.shutdown,
            wait=True,
        )

        logger.info(
            "Embedding scheduler stopped."
        )


app = FastAPI(
    title="Citibike RAG Backend API",
    description=(
        "Backend-only service for warehouse-grounded monthly "
        "trip questions and scheduled or on-demand "
        "embedding refreshes."
    ),
    lifespan=lifespan,
)


@app.post(
    "/ask",
    response_model=QuestionResponse,
)
def ask_endpoint(
    payload: QuestionRequest,
) -> QuestionResponse:
    try:
        result = ask_citibike_rag(
            payload.question,
        )

        # Validate here so an invalid pipeline result
        # is logged with a useful traceback.
        return QuestionResponse.model_validate(
            result,
        )

    except HTTPException:
        raise

    except Exception:
        logger.exception(
            "Question answering failed."
        )

        raise HTTPException(
            status_code=500,
            detail=(
                "Could not answer the question. "
                "Check the API server logs."
            ),
        ) from None


@app.post(
    "/embed/run",
    response_model=EmbeddingAccepted,
    status_code=202,
)
def trigger_embedding_on_demand(
    background_tasks: BackgroundTasks,
    request: Request,
) -> EmbeddingAccepted:
    manager: EmbeddingJobManager = (
        request.app.state.embedding_jobs
    )

    job = manager.reserve("manual")

    if job is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "An embedding refresh is already queued "
                "or running. Check /health for its job_id."
            ),
        )

    try:
        background_tasks.add_task(
            manager.run,
            job.job_id,
        )

    except Exception:
        manager.fail_to_start(
            job.job_id,
        )

        logger.exception(
            "Could not enqueue embedding job %s.",
            job.job_id,
        )

        raise HTTPException(
            status_code=500,
            detail=(
                "Could not start the embedding refresh."
            ),
        ) from None

    return EmbeddingAccepted(
        job_id=job.job_id,
        status_url=(
            f"/embed/jobs/{job.job_id}"
        ),
        triggered_at=job.created_at,
    )


@app.get(
    "/embed/jobs/{job_id}",
    response_model=EmbeddingJob,
)
def get_embedding_job(
    job_id: UUID,
    request: Request,
) -> EmbeddingJob:
    manager: EmbeddingJobManager = (
        request.app.state.embedding_jobs
    )

    job = manager.get(
        str(job_id),
    )

    if job is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Job not found. It may have expired "
                "or the API restarted."
            ),
        )

    return job


@app.get("/health")
def health_check(
    request: Request,
) -> dict[str, Any]:
    """
    Report API liveness and scheduler state.

    This endpoint does not test external service connections.
    """
    scheduler: BackgroundScheduler = (
        request.app.state.scheduler
    )

    manager: EmbeddingJobManager = (
        request.app.state.embedding_jobs
    )

    return {
        "status": "ok",
        "scheduler_running": scheduler.running,
        "schedule_timezone": "UTC",
        "scheduled_jobs": [
            {
                "id": job.id,
                "next_run": (
                    job.next_run_time.isoformat()
                    if job.next_run_time
                    else None
                ),
            }
            for job in scheduler.get_jobs()
        ],
        "embedding": manager.summary(),
    }