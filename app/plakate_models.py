from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.plakate_db import PlakatBase


class Plakat(PlakatBase):
    """Plakat-Standort in separater DB; User-IDs verweisen auf Haupt-DB (users)."""

    __tablename__ = "plakate"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    latitude: Mapped[float] = mapped_column(Float)
    longitude: Mapped[float] = mapped_column(Float)
    hung_by_user_id: Mapped[int] = mapped_column(Integer, index=True)
    hung_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    image_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    note: Mapped[str] = mapped_column(Text, default="")
    removed_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    removed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    @property
    def is_active(self) -> bool:
        return self.removed_at is None
