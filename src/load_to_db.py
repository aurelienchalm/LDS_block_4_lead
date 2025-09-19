import logging
from logging.handlers import RotatingFileHandler
from sqlalchemy import create_engine, Table, Column, Integer, Float, MetaData, insert
from sqlalchemy import DateTime, func
from sqlalchemy.orm import Session
import os
import pandas as pd
import boto3
from io import BytesIO
from dotenv import load_dotenv
from pathlib import Path

# ======================
# Logging configuration
# ======================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "load_to_db.log")

logger = logging.getLogger("load_to_db")
logger.setLevel(LOG_LEVEL)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)

# File handler (rotation)
fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)
fh.setLevel(LOG_LEVEL)

# Formatter
fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
ch.setFormatter(fmt)
fh.setFormatter(fmt)
logger.addHandler(ch)
logger.addHandler(fh)

# Réduire le bruit de libs verbeuses
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)  # mets INFO si tu veux voir les SQL

# ======================
# Config & env
# ======================
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path, override=True)

BUCKET_NAME = os.getenv("BUCKET_NAME")
CSV_KEY = os.getenv("CSV_KEY")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
table_name = "housing_prices"

def get_engine():
    # pool_pre_ping pour éviter les connexions mortes; echo=False pour ne pas spammer
    return create_engine(POSTGRES_DATABASE, future=True, pool_pre_ping=True)

def get_metadata():
    return MetaData()

def get_housing_table(metadata, name: str = "housing_prices"):
    return Table(
        name, metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('square_feet', Float),
        Column('num_bedrooms', Integer),
        Column('num_bathrooms', Integer),
        Column('num_floors', Integer),
        Column('year_built', Integer),
        Column('has_garden', Integer),
        Column('has_pool', Integer),
        Column('garage_size', Integer),
        Column('location_score', Float),
        Column('distance_to_center', Float),
        Column('price', Float),
        Column('price_predict', Float),
        Column('date_creation', DateTime, server_default=func.now()),
    )

def create_table_if_not_exists(engine, metadata, table):
    metadata.reflect(bind=engine)
    if table_name not in metadata.tables:
        logger.info(f"Table '{table_name}' absente → création…")
        metadata.create_all(engine)
        logger.info("Table créée")
    else:
        logger.info(f"Table '{table_name}' déjà existante.")

def fetch_csv_from_s3(bucket_name=BUCKET_NAME, key=CSV_KEY) -> pd.DataFrame:
    logger.info(f"Téléchargement depuis S3 s3://{bucket_name}/{key} …")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))
    df.columns = df.columns.str.lower()
    if 'id' in df.columns:
        df.drop(columns='id', inplace=True)
    logger.info(f"CSV chargé: shape={df.shape} | colonnes={list(df.columns)}")
    return df

def insert_data(df: pd.DataFrame, table, engine):
    if df.empty:
        logger.warning("DataFrame vide → aucune insertion.")
        return
    records = df.to_dict(orient='records')
    logger.info(f"Insertion de {len(records)} lignes dans {table.name}…")
    with Session(engine) as session:
        stmt = insert(table).values(records)
        session.execute(stmt)
        session.commit()
    logger.info("Insertion réussie")

def main():
    try:
        logger.info("Démarrage load_to_db.py")
        engine = get_engine()
        logger.info("Connexion Postgres OK")

        metadata = get_metadata()
        table = get_housing_table(metadata)
        create_table_if_not_exists(engine, metadata, table)

        df = fetch_csv_from_s3()
        insert_data(df, table, engine)

        logger.info("Terminé sans erreur")

    except Exception as e:
        # Log stack complète dans le fichier et la console
        logger.exception("Erreur lors de l'exécution du script")
        # Petit hint fréquent pour Neon/Postgres:
        logger.error(
            "Vérif POSTGRES_DATABASE (driver, host, port, sslmode). "
            "Ex: postgresql+psycopg://user:pass@host/db?sslmode=require"
        )
        raise  # laisse le code d'erreur remonter (utile en CI)

if __name__ == "__main__":
    main()