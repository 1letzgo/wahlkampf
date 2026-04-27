from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(512))


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    display_name: Mapped[str] = mapped_column(String(120), default="")
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_approved: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    termine: Mapped[List["Termin"]] = relationship(back_populates="creator")
    teilnahmen: Mapped[List["TerminTeilnahme"]] = relationship(back_populates="user")


class TerminTeilnahme(Base):
    __tablename__ = "termin_teilnahmen"
    __table_args__ = (
        UniqueConstraint("termin_id", "user_id", name="uq_teilnahme_termin_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    termin_id: Mapped[int] = mapped_column(ForeignKey("termine.id", ondelete="CASCADE"))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    termin: Mapped["Termin"] = relationship(back_populates="teilnahmen")
    user: Mapped["User"] = relationship(back_populates="teilnahmen")


class Termin(Base):
    __tablename__ = "termine"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200))
    description: Mapped[str] = mapped_column(Text, default="")
    vorbereitung: Mapped[str] = mapped_column(Text, default="")
    nachbereitung: Mapped[str] = mapped_column(Text, default="")
    location: Mapped[str] = mapped_column(String(300), default="")
    starts_at: Mapped[datetime] = mapped_column(DateTime)
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    image_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    externe_teilnehmer_json: Mapped[str] = mapped_column(Text, default="[]")
    created_by_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    creator: Mapped["User"] = relationship(back_populates="termine")
    teilnahmen: Mapped[List["TerminTeilnahme"]] = relationship(
        back_populates="termin",
        cascade="all, delete-orphan",
    )
