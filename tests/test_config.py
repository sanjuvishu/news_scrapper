import json

from news_ingestor.config import load_config


def test_load_config_uses_json_files(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    feeds_path = config_dir / "feeds.json"
    sectors_path = config_dir / "sectors.json"
    feeds_path.write_text(json.dumps(["https://example.com/feed"]), encoding="utf-8")
    sectors_path.write_text(json.dumps({"technology": ["ai"]}), encoding="utf-8")

    monkeypatch.setenv("RSS_FEEDS_FILE", str(feeds_path))
    monkeypatch.setenv("RSS_SECTORS_FILE", str(sectors_path))
    monkeypatch.setenv("MONGO_DB_NAME", "test_db")
    cfg = load_config(project_root=tmp_path)

    assert cfg.db_name == "test_db"
    assert cfg.feed_urls == ["https://example.com/feed"]
    assert cfg.sector_keywords["technology"] == ["ai"]

