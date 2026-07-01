"""Tests unitaires pour AcquisitionJobRecord (contracts) et AcquisitionJob (domain)."""

import pytest
from datetime import datetime, timezone

from civitas_acquisition.contracts.models.acquisition_job import (
    AcquisitionJobRecord,
    JobStatus,
    JobTrigger,
    new_job_id,
)
from civitas_acquisition.contracts.models.connector_manifest import ChannelType
from civitas_acquisition.domain.acquisition_job import AcquisitionJob, InvalidJobTransitionError
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.cursor import Cursor


def make_config() -> ConnectorConfig:
    return ConnectorConfig(
        instance_id="inst-github-1",
        connector_id="github",
        credentials={"token": "ghp_test"},
    )


def make_job(**overrides) -> AcquisitionJob:
    defaults = dict(
        connector_id="github",
        instance_id="inst-github-1",
        channel_type=ChannelType.POLLING,
        trigger=JobTrigger.SCHEDULED,
        config=make_config(),
    )
    defaults.update(overrides)
    return AcquisitionJob.create(**defaults)


# ── AcquisitionJobRecord ──────────────────────────────────────────────────────

class TestAcquisitionJobRecord:

    def _make_record(self, **overrides) -> AcquisitionJobRecord:
        defaults = dict(
            job_id=new_job_id(),
            connector_id="github",
            instance_id="inst-1",
            channel_type=ChannelType.POLLING,
            trigger=JobTrigger.SCHEDULED,
            status=JobStatus.COMPLETED,
            created_at=datetime.now(tz=timezone.utc),
            started_at=datetime.now(tz=timezone.utc),
            completed_at=datetime.now(tz=timezone.utc),
            documents_acquired=10,
            documents_skipped=2,
        )
        defaults.update(overrides)
        return AcquisitionJobRecord(**defaults)

    def test_est_immutable(self):
        record = self._make_record()
        with pytest.raises((AttributeError, TypeError)):
            record.documents_acquired = 99  # type: ignore[misc]

    def test_total_documents(self):
        record = self._make_record(
            documents_acquired=10,
            documents_skipped=3,
            documents_failed=1,
        )
        assert record.total_documents == 14

    def test_success_rate(self):
        record = self._make_record(
            documents_acquired=8,
            documents_skipped=0,
            documents_failed=2,
        )
        assert record.success_rate == pytest.approx(0.8)

    def test_success_rate_zero_total(self):
        record = self._make_record(
            documents_acquired=0,
            documents_skipped=0,
            documents_failed=0,
        )
        assert record.success_rate is None

    def test_is_terminal_completed(self):
        record = self._make_record(status=JobStatus.COMPLETED)
        assert record.is_terminal() is True

    def test_is_terminal_failed(self):
        record = self._make_record(status=JobStatus.FAILED)
        assert record.is_terminal() is True

    def test_is_not_terminal_pending(self):
        record = self._make_record(status=JobStatus.PENDING)
        assert record.is_terminal() is False

    def test_duration_ms_avec_timestamps(self):
        from datetime import timedelta
        start = datetime.now(tz=timezone.utc)
        end = start + timedelta(seconds=2.5)
        record = self._make_record(started_at=start, completed_at=end)
        assert record.duration_ms == pytest.approx(2500.0, rel=0.01)

    def test_duration_ms_sans_completed_at(self):
        record = self._make_record(status=JobStatus.RUNNING, completed_at=None)
        assert record.duration_ms is None


# ── AcquisitionJob (domain entity) ───────────────────────────────────────────

class TestAcquisitionJobLifecycle:

    def test_cree_en_pending(self):
        job = make_job()
        assert job.status == JobStatus.PENDING

    def test_start_transition_vers_running(self):
        job = make_job()
        job.start()
        assert job.status == JobStatus.RUNNING
        assert job.is_active is True

    def test_complete_depuis_running(self):
        job = make_job()
        job.start()
        job.complete()
        assert job.status == JobStatus.COMPLETED
        assert job.is_terminal is True

    def test_fail_depuis_running(self):
        job = make_job()
        job.start()
        job.fail("Network error")
        assert job.status == JobStatus.FAILED
        assert job.is_terminal is True

    def test_cancel_depuis_pending(self):
        job = make_job()
        job.cancel()
        assert job.status == JobStatus.CANCELLED

    def test_cancel_depuis_running(self):
        job = make_job()
        job.start()
        job.cancel()
        assert job.status == JobStatus.CANCELLED

    def test_transition_invalide_complete_vers_running(self):
        job = make_job()
        job.start()
        job.complete()
        with pytest.raises(InvalidJobTransitionError):
            job.start()

    def test_transition_invalide_pending_vers_completed(self):
        job = make_job()
        with pytest.raises(InvalidJobTransitionError):
            job.complete()

    def test_transition_invalide_failed_vers_running(self):
        job = make_job()
        job.start()
        job.fail("error")
        with pytest.raises(InvalidJobTransitionError):
            job.start()


class TestAcquisitionJobAccumulation:

    def test_increment_acquired(self):
        job = make_job()
        job.start()
        job.increment_acquired(5)
        job.increment_acquired(3)
        record = job.to_record()
        assert record.documents_acquired == 8

    def test_increment_skipped(self):
        job = make_job()
        job.start()
        job.increment_skipped(2)
        record = job.to_record()
        assert record.documents_skipped == 2

    def test_increment_failed(self):
        job = make_job()
        job.start()
        job.increment_failed(1)
        record = job.to_record()
        assert record.documents_failed == 1

    def test_advance_cursor(self):
        job = make_job()
        cursor = Cursor(
            value="2024-01-15T10:00:00Z",
            source_type="timestamp",
            connector_id="github",
            instance_id="inst-1",
        )
        job.start()
        job.advance_cursor(cursor)
        assert job.current_cursor == cursor

    def test_to_record_snapshot_complet(self):
        job = make_job()
        job.start()
        job.increment_acquired(10)
        job.increment_skipped(2)
        job.complete()

        record = job.to_record()
        assert record.connector_id == "github"
        assert record.instance_id == "inst-github-1"
        assert record.status == JobStatus.COMPLETED
        assert record.documents_acquired == 10
        assert record.documents_skipped == 2
        assert record.trigger == JobTrigger.SCHEDULED


# ── new_job_id ────────────────────────────────────────────────────────────────

class TestNewJobId:
    def test_genere_uuid_valide(self):
        import uuid
        job_id = new_job_id()
        uuid.UUID(job_id)   # lève ValueError si invalide

    def test_deux_ids_differents(self):
        assert new_job_id() != new_job_id()
