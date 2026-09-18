from pathlib import Path
import yaml

def load_config(config_path):
    project_root = Path(__file__).resolve().parents[2]
    config_file = project_root / config_path

    with open(config_file, "r") as file:
        return yaml.safe_load(file)