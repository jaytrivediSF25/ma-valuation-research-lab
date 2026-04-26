import json
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


try:
    import resource
except Exception:  # pragma: no cover
    resource = None


@dataclass
class RunSpan:
    name: str
    started_at: float
    ended_at: Optional[float] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return float(self.ended_at - self.started_at)


class RunLogger:
    def __init__(self, output_dir: Path) -> None:
        self.run_id = str(uuid.uuid4())
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.events: list[Dict[str, Any]] = []
        self.start_ts = time.time()

    def _memory_mb(self) -> Optional[float]:
        if resource is None:
            return None
        try:
            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if rss_kb > 10_000_000:  # macOS bytes behavior guard
                return float(rss_kb / (1024.0 * 1024.0))
            return float(rss_kb / 1024.0)
        except Exception:
            return None

    def log(self, event: str, payload: Dict[str, Any]) -> None:
        self.events.append(
            {
                "run_id": self.run_id,
                "event": event,
                "ts": time.time(),
                "memory_mb": self._memory_mb(),
                **payload,
            }
        )

    def timed(self, name: str):
        logger = self

        class _Timer:
            def __enter__(self_inner):
                self_inner.span = RunSpan(name=name, started_at=time.time())
                logger.log("stage_start", {"stage": name})
                return self_inner

            def __exit__(self_inner, exc_type, exc_val, exc_tb):
                self_inner.span.ended_at = time.time()
                payload: Dict[str, Any] = {
                    "stage": name,
                    "duration_seconds": self_inner.span.duration_seconds,
                    "status": "ok" if exc_type is None else "error",
                }
                if exc_type is not None:
                    payload["error"] = str(exc_val)
                    payload["traceback"] = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))[:3000]
                logger.log("stage_end", payload)
                return False

        return _Timer()

    def finalize(self, extra: Dict[str, Any]) -> Path:
        duration = float(time.time() - self.start_ts)
        report = {
            "run_id": self.run_id,
            "duration_seconds": duration,
            "event_count": len(self.events),
            "events": self.events,
            "summary": extra,
        }
        out = self.output_dir / f"run_observability_{self.run_id}.json"
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return out
