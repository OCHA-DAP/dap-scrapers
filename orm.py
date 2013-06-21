from sqlalchemy import Column, String, Boolean
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy import event


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
                
echo = False
engine = create_engine("sqlite:///ocha.db", echo=echo)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class Value(Base):
    __tablename__ = "value"
    dsID = Column(String, ForeignKey('dataset.dsID'), primary_key=True)
    region = Column(String, primary_key=True)
    indID = Column(String, ForeignKey('indicator.indID'), primary_key=True)
    period = Column(String, primary_key=True)
    value = Column(String, nullable=False)
    is_number = Column(Boolean)
    source = Column(String)


class DataSet(Base):
    __tablename__ = "dataset"
    dsID = Column(String, primary_key=True)
    last_updated = Column(String)
    last_scraped = Column(String)
    name = Column(String)


class Indicator(Base):
    __tablename__ = "indicator"
    indID = Column(String, primary_key=True)
    name = Column(String)
    units = Column(String)

Base.metadata.create_all(engine)


def send(klass, d, value_is_number=True):
    for item in d:
        try:
            d[item] = d[item].strip()
        except:
            pass
    return session.merge(klass(**d))
