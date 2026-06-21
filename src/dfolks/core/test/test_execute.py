"""Test for execute."""

from pathlib import Path
from typing import ClassVar

import pytest

from dfolks.core.classfactory import WorkflowsRegistry
from dfolks.core.execute import execute_job, execute_jobs, extract_job_yamls


class DummyWorkflow(WorkflowsRegistry):
    """Dummy workflow for executor tests."""

    wfclss: ClassVar[str] = "DummyWorkflow"
    kind: str = "DummyWorkflow"
    status: bool = True
    result: str = "done"
    run_count: int = 0

    def run(self):
        self.run_count += 1
        return self.result


def test_extract_job_yamls_extracts_yaml_files_without_subfolders(tmp_path):
    jobs_path = tmp_path / "jobs"
    samples_path = jobs_path / "Samples"
    samples_path.mkdir(parents=True)

    yaml_path = jobs_path / "b_job.yaml"
    yml_path = jobs_path / "a_job.yml"
    text_path = jobs_path / "not_job.txt"
    sample_path = samples_path / "sample_job.yaml"

    yaml_path.write_text("kind: BJob", encoding="utf-8")
    yml_path.write_text("kind: AJob", encoding="utf-8")
    text_path.write_text("kind: NotJob", encoding="utf-8")
    sample_path.write_text("kind: SampleJob", encoding="utf-8")

    assert extract_job_yamls(jobs_path) == [yml_path, yaml_path]


def test_extract_job_yamls_raises_if_jobs_path_does_not_exist(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract_job_yamls(tmp_path / "missing_jobs")


def test_extract_job_yamls_raises_if_jobs_path_is_not_folder(tmp_path):
    jobs_path = tmp_path / "jobs.yaml"
    jobs_path.write_text("kind: DummyWorkflow", encoding="utf-8")

    with pytest.raises(NotADirectoryError):
        extract_job_yamls(jobs_path)


def test_execute_job_loads_yaml_and_runs_workflow(tmp_path, monkeypatch):
    job_path = tmp_path / "job.yaml"
    job_path.write_text("kind: DummyWorkflow\nresult: complete\n", encoding="utf-8")
    loaded_jobs = []

    def fake_load_class(job):
        loaded_jobs.append(job)
        return DummyWorkflow(result="complete")

    monkeypatch.setattr("dfolks.core.execute.load_class", fake_load_class)

    assert execute_job(job_path) == "complete"
    assert loaded_jobs == [{"kind": "DummyWorkflow", "result": "complete"}]


def test_execute_job_skips_workflow_when_status_is_false(tmp_path, monkeypatch):
    job_path = tmp_path / "job.yaml"
    job_path.write_text("kind: DummyWorkflow\nstatus: false\n", encoding="utf-8")

    def fake_load_class(job):
        return DummyWorkflow(status=job["status"])

    monkeypatch.setattr("dfolks.core.execute.load_class", fake_load_class)

    assert execute_job(job_path) is None


def test_execute_job_raises_if_loaded_class_is_not_workflow(tmp_path, monkeypatch):
    job_path = tmp_path / "job.yaml"
    job_path.write_text("kind: NotWorkflow\n", encoding="utf-8")

    monkeypatch.setattr("dfolks.core.execute.load_class", lambda yml: object())

    with pytest.raises(TypeError) as excinfo:
        execute_job(job_path)

    assert "not a workflow" in str(excinfo.value)


def test_execute_jobs_executes_one_by_one_and_continues_after_failure(
    tmp_path, monkeypatch
):
    jobs_path = tmp_path / "jobs"
    jobs_path.mkdir()

    first_job = jobs_path / "a_job.yaml"
    failed_job = jobs_path / "b_job.yaml"
    second_job = jobs_path / "c_job.yaml"
    first_job.write_text("kind: FirstWorkflow\n", encoding="utf-8")
    failed_job.write_text("kind: FailedWorkflow\n", encoding="utf-8")
    second_job.write_text("kind: SecondWorkflow\n", encoding="utf-8")

    executed_jobs = []

    def fake_execute_job(job_path):
        executed_jobs.append(Path(job_path).name)
        if Path(job_path).name == "b_job.yaml":
            raise ValueError("invalid job")
        return Path(job_path).stem

    monkeypatch.setattr("dfolks.core.execute.execute_job", fake_execute_job)

    assert execute_jobs(jobs_path) is None
    assert executed_jobs == ["a_job.yaml", "b_job.yaml", "c_job.yaml"]
