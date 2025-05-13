"""
data_cleaning.py

This module provides data cleaning functions for user and login datasets
as part of the UK Data Pipeline exercise. It includes normalization,
validation, transformation, and obfuscation of sensitive fields.
"""

import re
from hashlib import sha256
from datetime import datetime

def clean_column_names(df):
    """
    Clean all columns names so that they:
     - are lower case
     - no whitespace
     - replace spaces with underscores
    """
    try:
        df.columns = [x.lower().strip().replace(' ', '_') for x in df.columns]
        return df
    except Exception:
        return None    

def clean_middle_initials(value):
    """Cleans the middle initials field by removing placeholder values."""
    if isinstance(value, str) and value.strip().lower() in ['none', '-', '{null}', '']:
        return None
    return value.strip() if isinstance(value, str) else None


def clean_dob(dob_str, age_last_birthday):
    """
    Converts DoB in DD/MM/YY format to YYYY-MM-DD using age to infer century.
    Assumes DoB has no leading zeros and is in UK format.
    """
    try:
        day, month, year = map(int, dob_str.split('/'))
        current_year = datetime.now().year
        full_year = current_year - int(age_last_birthday)
        return datetime(full_year, month, day).strftime('%Y-%m-%d')
    except Exception:
        return None


def clean_gender(value):
    """Normalizes gender values and replaces blanks with None."""
    if not isinstance(value, str):
        return None
    val = value.strip().lower()
    if val in ['-', 'blank', '']:
        return None
    return value.strip()


def hash_password(password):
    """Hashes passwords using SHA-256. Replace with bcrypt for real-world usage."""
    if isinstance(password, str):
        return sha256(password.encode('utf-8')).hexdigest()
    return None


def clean_salary(value):
    """Parses salary, removing commas and converting to float. Returns None for 'na'."""
    if not isinstance(value, str):
        return None
    if value.strip().lower() == 'na':
        return None
    try:
        return float(value.replace(',', ''))
    except ValueError:
        return None


def clean_postcode(value):
    """
    Validates and formats UK postcodes. Returns None if not matching.
    Example valid: 'EC1A 1BB', 'W1A 0AX'
    """
    if not isinstance(value, str):
        return None
    postcode = value.strip().upper()
    if re.match(r'^[A-Z]{1,2}\d{1,2} ?\d[A-Z]{2}$', postcode):
        return postcode
    return None


def convert_epoch_to_iso(timestamp):
    """Converts Unix epoch timestamp to ISO 8601 UTC format."""
    try:
        return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


def generate_uid(email):
    """Generates a unique ID from an email address."""
    if not isinstance(email, str):
        return None
    return sha256(email.strip().lower().encode('utf-8')).hexdigest()
