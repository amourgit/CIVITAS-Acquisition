"""Tests unitaires pour RetryEngine et CircuitBreaker."""

import pytest
from civitas_acquisition.runtime.resilience.retry_engine import RetryEngine
from civitas_acquisition.runtime.resilience.circuit_breaker import (
    CircuitBreaker, CircuitState,
)
from civitas_acquisition.policies.retry_policy import RetryPolicy, NO_RETRY
from civitas_acquisition.contracts.errors.resilience_errors import (
    MaxRetriesExhaustedError,
    CircuitOpenError,
)
from civitas_acquisition.contracts.errors.connector_errors import ConnectorNetworkError


class TestRetryEngine:

    async def test_succes_du_premier_coup(self):
        engine = RetryEngine(RetryPolicy(max_attempts=3, initial_delay_ms=0))
        calls = []
        async def fn():
            calls.append(1)
            return "ok"
        result = await engine.execute(fn)
        assert result == "ok"
        assert len(calls) == 1

    async def test_retry_sur_erreur_retryable(self):
        engine = RetryEngine(RetryPolicy(
            max_attempts=3, initial_delay_ms=0, jitter=False,
            retryable_errors=(ConnectorNetworkError,),
        ))
        calls = []
        async def fn():
            calls.append(1)
            if len(calls) < 3:
                raise ConnectorNetworkError("rss", cause="timeout")
            return "ok"
        result = await engine.execute(fn)
        assert result == "ok"
        assert len(calls) == 3

    async def test_max_retries_exhausted(self):
        engine = RetryEngine(RetryPolicy(
            max_attempts=2, initial_delay_ms=0, jitter=False,
            retryable_errors=(ConnectorNetworkError,),
        ))
        async def fn():
            raise ConnectorNetworkError("rss", cause="always fails")
        with pytest.raises(MaxRetriesExhaustedError) as exc_info:
            await engine.execute(fn)
        assert exc_info.value.attempts == 2

    async def test_erreur_fatale_pas_retryable(self):
        engine = RetryEngine(RetryPolicy(
            max_attempts=3, initial_delay_ms=0,
            retryable_errors=(ConnectorNetworkError,),
        ))
        calls = []
        async def fn():
            calls.append(1)
            raise ValueError("fatal — should not retry")
        with pytest.raises(ValueError):
            await engine.execute(fn)
        assert len(calls) == 1

    async def test_no_retry_policy(self):
        engine = RetryEngine(NO_RETRY)
        calls = []
        async def fn():
            calls.append(1)
            raise ConnectorNetworkError("rss")
        with pytest.raises(MaxRetriesExhaustedError):
            await engine.execute(fn)
        assert len(calls) == 1


class TestCircuitBreaker:

    def _make_cb(self, threshold=3, recovery=0.01) -> CircuitBreaker:
        return CircuitBreaker(
            resource_id="test-resource",
            failure_threshold=threshold,
            recovery_timeout_s=recovery,
            half_open_max_calls=2,
        )

    async def test_closed_par_defaut(self):
        cb = self._make_cb()
        assert cb.state == CircuitState.CLOSED

    async def test_succes_reste_closed(self):
        cb = self._make_cb()
        async def ok(): return "ok"
        await cb.call(ok)
        assert cb.state == CircuitState.CLOSED

    async def test_failures_ouvrent_le_circuit(self):
        cb = self._make_cb(threshold=2)
        async def fail(): raise ConnectorNetworkError("res")
        for _ in range(2):
            with pytest.raises(ConnectorNetworkError):
                await cb.call(fail)
        assert cb.state == CircuitState.OPEN

    async def test_circuit_open_bloque_appels(self):
        cb = self._make_cb(threshold=1)
        async def fail(): raise ConnectorNetworkError("res")
        with pytest.raises(ConnectorNetworkError):
            await cb.call(fail)
        async def ok(): return "ok"
        with pytest.raises(CircuitOpenError):
            await cb.call(ok)

    async def test_reset_manuel_ferme_le_circuit(self):
        cb = self._make_cb(threshold=1)
        async def fail(): raise ConnectorNetworkError("res")
        with pytest.raises(ConnectorNetworkError):
            await cb.call(fail)
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
