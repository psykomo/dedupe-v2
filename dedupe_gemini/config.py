import yaml
import os
from pathlib import Path
from typing import Dict, Any

DEFAULT_CONFIG = {
    "database": {
        "url": "mysql+pymysql://root:root@127.0.0.1:3306/sdp_pusat"
    },
    "seeding": {
        "default_batch_size": 1000,
        "default_duplicates": 0.05
    },
    "deduplication": {
        "threshold": 0.9,
        "blocking_rules": []
    }
}

def load_config(config_path: str = "config.yml") -> Dict[str, Any]:
    """
    Load configuration from a YAML file. If not found, returns default config.
    """
    path = Path(config_path)
    if not path.exists():
        # Fallback to looking in current directory if absolute path fails
        path = Path.cwd() / config_path
        
    if path.exists():
        with open(path, "r") as f:
            try:
                user_config = yaml.safe_load(f)
                # Simple merge: user config overrides defaults
                # A deep merge would be better for nested dicts, but this suffices for now
                config = DEFAULT_CONFIG.copy()
                config.update(user_config)
                
                # Ensure nested keys are preserved if only partial config provided
                if "database" in user_config:
                    config["database"] = {**DEFAULT_CONFIG["database"], **user_config["database"]}
                if "seeding" in user_config:
                    config["seeding"] = {**DEFAULT_CONFIG["seeding"], **user_config["seeding"]}
                    
                return config
            except yaml.YAMLError as exc:
                print(f"Error parsing config file: {exc}")
                return DEFAULT_CONFIG
    
    return DEFAULT_CONFIG
