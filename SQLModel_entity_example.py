# from __future__ import annotations
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import UniqueConstraint
from .utils import SCHEMA_NAME
from app.models.core.utils import SCHEMA_NAME as CORE_SCHEMA_NAME
from typing import TYPE_CHECKING
from datetime import date


if TYPE_CHECKING:
    from app.models.core import Product
    from app.models.core import Channel


class MPLeftoverBase(SQLModel):
    last_updated_date: date = Field(default=None, nullable=False)
    product_id: int = Field(
        default=None,
        nullable=False,
        foreign_key=f"{CORE_SCHEMA_NAME}.products.id"
    )
    channel_id: int = Field(
        default=None,
        nullable=False,
        foreign_key=f"{CORE_SCHEMA_NAME}.channels.id"
    )
    amount: int = Field(default=None, nullable=False)
    warehouse: str = Field(default=None, nullable=False)


class MPLeftover(MPLeftoverBase, table=True):
    __table_args__ = (
        UniqueConstraint(
            "last_updated_date", "product_id", "channel_id"
        ),
        {"schema": SCHEMA_NAME}
    )
    __tablename__ = "leftovers"

    id: int = Field(default=None, nullable=False, primary_key=True)

    product: "Product" = Relationship(  # ссылаемся продукт скю если смотрим разрез не по продуктам а по отдельные скю источников
        back_populates="mp_leftovers"
    )

    channel: "Channel" = Relationship(
        back_populates="mp_leftovers"
    )


class MPLeftoverCreate(MPLeftoverBase):
    pass
