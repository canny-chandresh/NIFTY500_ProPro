from pathlib import Path
import json

def test_catalog_exists():
    cat = Path("reports/auto_features/catalog.json")
    assert cat.exists(), "Run feature_factory.run_universe() first to create catalog"
    data = json.loads(cat.read_text())
    assert isinstance(data, list), "catalog should be a JSON list"
