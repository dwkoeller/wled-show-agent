from __future__ import annotations

from utils.rate_limit_metrics import RateLimitPrometheusMetrics


def test_rate_limit_metrics_render() -> None:
    reg = RateLimitPrometheusMetrics()
    reg.observe(scope="ip", decision="allowed")
    reg.observe(scope="ip", decision="blocked")
    output = reg.render()
    assert "wsa_rate_limit_requests_total" in output
    assert 'decision="allowed"' in output
    assert 'decision="blocked"' in output
