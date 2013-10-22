from sqlalchemy import Column, String, Boolean
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy import event
import datetime
from canon import canonicalise, canon_number, canon_period, chd_id
import atexit


def now():
    return datetime.datetime.now().isoformat()


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

    def is_blank(self):
        if type(self.value) in [float, int]:
            return False
        return self.value is None or self.value.strip() == ''

    def save(self):
        self.region = canonicalise(self.region)
        self.period = canon_period(self.period)
        self.indID = chd_id(self.indID)
        if self.region is None:
            return
        if self.is_number:
            self.value = canon_number(self.value)
            if self.value is None:
                return
        assert not self.is_blank()
        try:
            session.merge(self)
        except:
            print self.__dict__
            raise


class DataSet(Base):
    __tablename__ = "dataset"
    dsID = Column(String, primary_key=True)
    last_updated = Column(String)
    last_scraped = Column(String)
    name = Column(String)

    def save(self):
        session.merge(self)


class Indicator(Base):
    __tablename__ = "indicator"
    indID = Column(String, primary_key=True)
    name = Column(String)
    units = Column(String)

    def save(self):
        self.indID = chd_id(self.indID)
        session.merge(self)

Base.metadata.create_all(engine)


@atexit.register
def exithandler():
    session.commit()
