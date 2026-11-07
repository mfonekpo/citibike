from dotenv import load_dotenv
import os

load_dotenv()


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")

# Superset PostgreSQL metadata database
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:"
    f"{os.getenv('SUPERSET_DB_PASSWORD')}"
    "@superset-db:5432/superset"
)

# Maximum number of rows returned by default
ROW_LIMIT = 5000