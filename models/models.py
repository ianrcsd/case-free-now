from datetime import datetime
from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel


class PassengerBase(SQLModel):
    id: str = Field(default=None, primary_key=True)
    date_registered: Optional[str]
    country_code: Optional[str]


class PassengerBronze(PassengerBase, table=True):
    __tablename__ = "passenger_bronze"


class PassengerSilver(PassengerBase, table=True):
    __tablename__ = "passenger_silver"
    id: int = Field(default=None, primary_key=True)
    date_registered: datetime
    bookings: List["BookingSilver"] = Relationship(back_populates="passenger")


class PassengerGold(PassengerBase, table=True):
    __tablename__ = "passenger_gold"
    id: int = Field(default=None, primary_key=True)
    date_registered: datetime
    bookings: List["BookingGold"] = Relationship(back_populates="passenger")


class BookingBase(SQLModel):
    id: str = Field(default=None, primary_key=True)
    id_passenger: str
    date_created: Optional[str]
    date_close: Optional[str]


class BookingBronze(BookingBase, table=True):
    __tablename__ = "booking_bronze"


class BookingSilver(BookingBase, table=True):
    __tablename__ = "booking_silver"
    id: int = Field(default=None, primary_key=True)
    id_passenger: int = Field(default=None, foreign_key="passenger_silver.id")
    date_created: datetime
    date_close: datetime
    passenger: PassengerSilver = Relationship(back_populates="bookings")


class BookingGold(BookingBase, table=True):
    __tablename__ = "booking_gold"
    id: int = Field(default=None, primary_key=True)
    id_passenger: int = Field(default=None, foreign_key="passenger_gold.id")
    date_created: datetime
    date_close: datetime
    passenger: PassengerGold = Relationship(back_populates="bookings")
