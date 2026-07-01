"""CIVITAS Acquisition — Models. Import from here."""

from .cursor import Cursor, CursorSourceType
from .raw_document import RawDocument, SourceReference, make_document_id, sha256_checksum
from .connector_manifest import (
    ConnectorManifest, ChannelType, SourceCategory, RateLimit, CredentialSpec,
)
from .connector_config import ConnectorConfig
from .health_status import HealthStatus
from .discovery_result import DiscoveryResult
from .acquisition_job import (
    AcquisitionJobRecord, JobStatus, JobTrigger, new_job_id,
)
from .events import (
    AcquisitionEvent,
    RawDocumentCreated,
    AcquisitionFailed,
    DocumentDeduplicated,
    CursorAdvanced,
    ConnectorHealthChanged,
    CircuitBreakerStateChanged,
    DLQDocumentEnqueued,
)

__all__ = [
    "Cursor", "CursorSourceType",
    "RawDocument", "SourceReference", "make_document_id", "sha256_checksum",
    "ConnectorManifest", "ChannelType", "SourceCategory", "RateLimit", "CredentialSpec",
    "ConnectorConfig",
    "HealthStatus",
    "DiscoveryResult",
    "AcquisitionJobRecord", "JobStatus", "JobTrigger", "new_job_id",
    "AcquisitionEvent", "RawDocumentCreated", "AcquisitionFailed",
    "DocumentDeduplicated", "CursorAdvanced", "ConnectorHealthChanged",
    "CircuitBreakerStateChanged", "DLQDocumentEnqueued",
]
