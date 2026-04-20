from services import radar_service


def assert_radar_contract(result):
    assert isinstance(result, dict)
    assert "status" in result
    assert "opportunities" in result
    assert "ready_count" in result
    assert "total_count" in result
    assert "low_data_mode" in result
    assert isinstance(result["opportunities"], list)
    assert isinstance(result["ready_count"], int)
    assert isinstance(result["total_count"], int)
    assert isinstance(result["low_data_mode"], bool)


def test_get_investment_opportunities_empty_result_keeps_contract(monkeypatch):
    monkeypatch.setattr(radar_service, "radar_get_radar_ready_count", lambda: 7)
    monkeypatch.setattr(radar_service, "radar_get_top_opportunities", lambda limit=20: [])

    result = radar_service.get_investment_opportunities(limit=5)

    assert_radar_contract(result)
    assert result["status"] == "insufficient_data"
    assert result["ready_count"] == 7
    assert result["total_count"] == 0
    assert result["low_data_mode"] is True
    assert result["opportunities"] == []


def test_get_investment_opportunities_normal_result_keeps_contract(monkeypatch):
    opportunities = [
        {
            "listing_id": 123,
            "comuna": "nunoa",
            "investment_score": 82,
        }
    ]

    monkeypatch.setattr(radar_service, "radar_get_radar_ready_count", lambda: 42)
    monkeypatch.setattr(
        radar_service,
        "radar_get_top_opportunities",
        lambda limit=20: opportunities,
    )

    result = radar_service.get_investment_opportunities(limit=5)

    assert_radar_contract(result)
    assert result["status"] == "ok"
    assert result["ready_count"] == 42
    assert result["total_count"] == 1
    assert result["low_data_mode"] is False
    assert result["opportunities"] == opportunities


def test_validate_or_normalize_radar_result_fills_missing_keys():
    result = radar_service.validate_or_normalize_radar_result({"status": "ok"})

    assert_radar_contract(result)
    assert result["opportunities"] == []
    assert result["ready_count"] == 0
    assert result["total_count"] == 0
    assert result["low_data_mode"] is False


def test_validate_or_normalize_radar_result_handles_non_dict():
    result = radar_service.validate_or_normalize_radar_result(None)

    assert_radar_contract(result)
    assert result["opportunities"] == []
    assert result["ready_count"] == 0
    assert result["total_count"] == 0
    assert result["low_data_mode"] is False
