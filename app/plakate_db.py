from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import PLAKATE_DATABASE_URL


class PlakatBase(DeclarativeBase):
    pass


_connect_args = (
    {"check_same_thread": False}
    if PLAKATE_DATABASE_URL.startswith("sqlite")
    else {}
)

plakate_engine = create_engine(
    PLAKATE_DATABASE_URL,
    connect_args=_connect_args,
)
PlakatSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=plakate_engine,
)


def get_plakate_db():
    db = PlakatSessionLocal()
    try:
        yield db
    finally:
        db.close()
