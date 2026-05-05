from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class PlatformBase(DeclarativeBase):
    pass


class Ortsverband(PlatformBase):
    """Registrierter Ortsverband (Slug = URL-Pfad unter /m/<slug>/)."""

    __tablename__ = "ortsverbaende"

    slug: Mapped[str] = mapped_column(String(80), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(200), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class PlatformUser(PlatformBase):
    """Globaler Account (ein Benutzername für alle OVs)."""

    __tablename__ = "platform_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    display_name: Mapped[str] = mapped_column(String(120), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    calendar_token: Mapped[Optional[str]] = mapped_column(
        String(64), unique=True, nullable=True, index=True
    )

    memberships: Mapped[List["OvMembership"]] = relationship(back_populates="user")


class OvMembership(PlatformBase):
    """Zuordnung Nutzer ↔ OV inkl. Freigabe und OV-Admin."""

    __tablename__ = "ov_memberships"
    __table_args__ = (UniqueConstraint("user_id", "ov_slug", name="uq_membership_user_ov"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("platform_users.id", ondelete="CASCADE"))
    ov_slug: Mapped[str] = mapped_column(
        String(80), ForeignKey("ortsverbaende.slug", ondelete="CASCADE"), index=True
    )
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_approved: Mapped[bool] = mapped_column(Boolean, default=False)

    user: Mapped["PlatformUser"] = relationship(back_populates="memberships")


class Termin(PlatformBase):
    """Termin mandantenbezogen (ein OV pro Zeile über mandant_slug)."""

    __tablename__ = "termine"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mandant_slug: Mapped[str] = mapped_column(
        String(80), ForeignKey("ortsverbaende.slug", ondelete="CASCADE"), index=True
    )
    title: Mapped[str] = mapped_column(String(200))
    description: Mapped[str] = mapped_column(Text, default="")
    location: Mapped[str] = mapped_column(String(300), default="")
    starts_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    image_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    externe_teilnehmer_json: Mapped[str] = mapped_column(Text, default="[]")
    created_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("platform_users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    promoted_all_ovs: Mapped[bool] = mapped_column(Boolean, default=False)
    attachments_json: Mapped[str] = mapped_column(Text, default="[]")

    creator: Mapped[Optional["PlatformUser"]] = relationship()
    teilnahmen: Mapped[List["TerminTeilnahme"]] = relationship(
        back_populates="termin",
        cascade="all, delete-orphan",
    )
    kommentare: Mapped[List["TerminKommentar"]] = relationship(
        back_populates="termin",
        cascade="all, delete-orphan",
    )


TEILNAHME_STATUS_ZUGESAGT = "zugesagt"
TEILNAHME_STATUS_ABGESAGT = "abgesagt"


class TerminTeilnahme(PlatformBase):
    __tablename__ = "termin_teilnahmen"
    __table_args__ = (
        UniqueConstraint("termin_id", "user_id", name="uq_teilnahme_termin_user_plat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    termin_id: Mapped[int] = mapped_column(ForeignKey("termine.id", ondelete="CASCADE"))
    user_id: Mapped[int] = mapped_column(ForeignKey("platform_users.id", ondelete="CASCADE"))
    teilnahme_status: Mapped[str] = mapped_column(
        String(16),
        default=TEILNAHME_STATUS_ZUGESAGT,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    termin: Mapped["Termin"] = relationship(back_populates="teilnahmen")
    user: Mapped["PlatformUser"] = relationship()


class TerminKommentar(PlatformBase):
    __tablename__ = "termin_kommentare"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    termin_id: Mapped[int] = mapped_column(
        ForeignKey("termine.id", ondelete="CASCADE"), index=True
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("platform_users.id", ondelete="CASCADE"))
    body: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    termin: Mapped["Termin"] = relationship(back_populates="kommentare")
    user: Mapped["PlatformUser"] = relationship()


class MandantAppSetting(PlatformBase):
    """Schlüssel/Wert pro OV (z. B. founder_done, öffentlicher ICS-Token)."""

    __tablename__ = "mandant_app_settings"

    mandant_slug: Mapped[str] = mapped_column(
        String(80),
        ForeignKey("ortsverbaende.slug", ondelete="CASCADE"),
        primary_key=True,
    )
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(512))


class MandantPlakat(PlatformBase):
    """Plakat-Standort je OV; User-IDs referenzieren platform_users.id (ohne FK)."""

    __tablename__ = "mandant_plakate"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mandant_slug: Mapped[str] = mapped_column(
        String(80),
        ForeignKey("ortsverbaende.slug", ondelete="CASCADE"),
        index=True,
    )
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
