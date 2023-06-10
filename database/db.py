from contextlib import contextmanager

import sqlalchemy
from models import SQLDeclarativeBase as Base
from sqlalchemy.orm import sessionmaker

user = "<Your database user name>"
password = "<Your database pasowrd>"
uri = "<Your database uri>"
db = "<Your database name>"

postgres_uri = f"postgresql://{user}:{password}@{uri}/{db}?sslmode=require"

engine = sqlalchemy.create_engine(postgres_uri)

Base.metadata.create_all(engine)


Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_session():
    session = Session()
    try:
        yield session
    finally:
        session.close()
