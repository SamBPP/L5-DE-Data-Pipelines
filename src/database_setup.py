"""
database_setup.py

This module defines the SQLAlchemy ORM schema and creates the database.
It supports creating an SQLite database with tables for users and login activity.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Create the declarative base
Base = declarative_base()

# Define the Users table
class User(Base):
    __tablename__ = 'users'

    id = Column(String, primary_key=True)  # UID
    first_name = Column(String, nullable=False)
    middle_initials = Column(String, nullable=True)
    surname = Column(String, nullable=False)
    dob = Column(Date, nullable=False)
    gender = Column(String, nullable=True)
    favourite_colour = Column(String)
    favourite_animal = Column(String)
    favourite_food = Column(String)
    city = Column(String)
    county = Column(String)
    postcode = Column(String)
    email = Column(String, nullable=False)
    phone = Column(String)
    mobile = Column(String)
    rqf = Column(String)
    salary = Column(Float)
    password_hash = Column(String)

    # Relationship to login entries
    logins = relationship("Login", back_populates="user")

# Define the Login audit table
class Login(Base):
    __tablename__ = 'logins'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    login_ts = Column(DateTime, nullable=False)

    user = relationship("User", back_populates="logins")


def create_db(db_path):
    """Creates the SQLite database and tables."""
    engine = create_engine(db_path)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine):
    """Creates a new SQLAlchemy session."""
    Session = sessionmaker(bind=engine)
    return Session()