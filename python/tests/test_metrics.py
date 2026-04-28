import pytest
from app.metrics import ingest_metrics


@pytest.mark.asyncio
async def test_ingest_metrics():
    response = await ingest_metrics('{"project_id": "abc123", "metrics": {}}')

    assert response["status"] == 200
    assert response["project_id"] == "abc123"
    assert response["fingerprint_id"].startswith("fp_")
    assert "request_id" in response
