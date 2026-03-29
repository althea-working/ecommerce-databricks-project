import os
import yaml

def load_config():

    current_file = os.path.abspath(__file__)

    project_root = os.path.dirname(
        os.path.dirname(
            os.path.dirname(current_file)
        )
    )

    config_path = os.path.join(project_root, "conf", "config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found at: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)