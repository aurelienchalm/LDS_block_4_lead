import os
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path, override=True)

DATABASE_URL = os.getenv("POSTGRES_DATABASE")

engine = create_engine(DATABASE_URL, echo=True)