"""Execute workflow jobs from YAML files.

Need to work:
0) Documentation.
1) Add CLI options.
"""

import logging
from pathlib import Path
from typing import List, Optional

import yaml

from dfolks.core.classfactory import WorkflowsRegistry, load_class
from dfolks.core.modules import set_logger

# Set up shared logger
logger = logging.getLogger("shared")


def extract_job_yamls(jobs_path: str | Path = "jobs") -> List[Path]:
    """Extract YAML files directly under the jobs folder."""
    jobs_dir = Path(jobs_path)

    if not jobs_dir.exists():
        raise FileNotFoundError(f"Jobs folder does not exist: {jobs_dir}")

    if not jobs_dir.is_dir():
        raise NotADirectoryError(f"Jobs path is not a folder: {jobs_dir}")

    job_yamls = sorted(
        [
            path
            for pattern in ("*.yaml", "*.yml")
            for path in jobs_dir.glob(pattern)
            if path.is_file()
        ]
    )

    logger.info(f"Extracted {len(job_yamls)} YAML files from {jobs_dir}.")
    return job_yamls


def execute_job(job_path: str | Path) -> Optional[object]:
    """Load and execute a single job YAML file."""
    job_path = Path(job_path)

    logger.info(f"Loading job YAML: {job_path}")
    job_yaml = yaml.safe_load(job_path.read_text(encoding="utf-8"))
    job = load_class(job_yaml)

    if not isinstance(job, WorkflowsRegistry):
        raise TypeError(f"Loaded class is not a workflow: {job_path}")

    if job.status:
        logger.info(f"Executing workflow: {job_path}")
        result = job.run()
        logger.info(f"Completed workflow: {job_path}")

        return result
    else:
        logger.info(f"Skipping job YAML (status=False): {job_path}")
        return None


def execute_jobs(jobs_path: str | Path = "jobs") -> None:
    """Execute all runnable YAML jobs directly under the jobs folder."""
    logger.info("Starting job execution.")
    job_yamls = extract_job_yamls(jobs_path)

    for job_path in job_yamls:
        try:
            _ = execute_job(job_path)
        except Exception:
            logger.exception(f"Failed to execute job YAML: {job_path}")

    logger.info("Job execution completed.")


if __name__ == "__main__":
    logger = set_logger("shared", logging.INFO, None)
    execute_jobs()
