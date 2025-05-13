"""
pipeline_runner.py

Main controller script for the UK Data Pipeline project.
Loads, cleans, and inserts user and login data into the database.
"""

import pandas as pd
import data_cleaning as dc
from database_setup import create_db, get_session, User, Login
from datetime import datetime
import os

# File paths
USER_CSV_PATH = "data/UK User Data.csv"
LOGIN_CSV_PATH = "data/UK-User-LoginTS.csv"
DB_PATH = "sqlite:///databases/user_data.db"

def ensure_directory_exists(directory):
    """Ensures the directory exists, creating it if necessary."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def load_csv(filepath, encoding=None):
    if encoding is None:
        return pd.read_csv(filepath)
    else:
        return pd.read_csv(filepath, encoding=encoding)

def process_users(df):
    # clean column headers
    df = dc.clean_column_names(df)
    users = []
    for _, row in df.iterrows():
        
        uid = dc.generate_uid(row['email'])

        dob = dc.clean_dob(row['dob'], row['age_last_birthday'])

        user = User(
            id=uid,
            first_name=row['first_name'].strip(),
            middle_initials=dc.clean_middle_initials(row['middle_initials']),
            surname=row['surname'].strip(),
            dob=datetime.strptime(dob, '%Y-%m-%d').date() if dob else None,
            gender=dc.clean_gender(row['gender']),
            favourite_colour=row['favourite_colour'],
            favourite_animal=row['favourite_animal'],
            favourite_food=row['favourite_food'],
            city=row['city'],
            county=row['county'],
            postcode=dc.clean_postcode(row['postcode']),
            email=row['email'],
            phone=row['phone'],
            mobile=row['mobile'],
            rqf=row['rqf'] if pd.notnull(row['rqf']) else None,
            salary=dc.clean_salary(str(row['salary'])),
            password_hash=dc.hash_password(row['password'])
        )
        users.append(user)
    return users


def process_logins(df, email_to_uid_map):
    logins = []
    df = dc.clean_column_names(df)
    for _, row in df.iterrows():
        uid = email_to_uid_map.get(row['username'].strip())
        if not uid:
            continue
        timestamp = dc.convert_epoch_to_iso(row['logints'])
        if timestamp:
            login = Login(
                user_id=uid,
                login_ts=datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            )
            logins.append(login)
    return logins


def main():
    # Ensure the database directory exists
    ensure_directory_exists('databases')

    # Set up database
    engine = create_db(DB_PATH)
    session = get_session(engine)

    # Load and clean data
    df_users = load_csv(USER_CSV_PATH, encoding='latin1')
    df_logins = load_csv(LOGIN_CSV_PATH)

    # Clean and transform user data
    users = process_users(df_users)
    email_to_uid = {u.email: u.id for u in users}
    logins = process_logins(df_logins, email_to_uid)

    # Insert into database
    session.add_all(users)
    session.commit()

    session.add_all(logins)
    session.commit()

    print("Data pipeline executed successfully!")
    
    # Close the session
    session.close()

if __name__ == "__main__":
    main()