from sqlalchemy import create_engine
import os
from dedupe_gemini.config import load_config

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        config = load_config()
        # Allow env variable to override config
        db_url = os.getenv("DATABASE_URL", config.get("database", {}).get("url"))
        _engine = create_engine(db_url)
    return _engine
