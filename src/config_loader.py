# src/config_loader.py
import yaml

def load_sources_config(path: str = "config/sources.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data
