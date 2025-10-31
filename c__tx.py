from contextlib import contextmanager
from typing import Any, List

from dependency_injector.wiring import inject
from sqlalchemy import create_engine, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker


class SingletonMetaClass(type):
    """
    SingletonMetaClass is for implementing singleton, pass in to the class by using metaclass=SingletonMetaClass
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMetaClass, cls).__call__(
                *args, **kwargs
            )
        return cls._instances[cls]


class Context(metaclass=SingletonMetaClass):
    """This class manages the database sessions"""

    def __init__(self):
        self.db_base = declarative_base()
        self.conn = ""

    def init_session(self, connection_str: str = "sqlite:///"):
        """set up the database connections, it will use sqlite if no connection_str is used

        Args:
            connection_str (str, optional): connection string to the db. Defaults to "sqlite:///".
        """
        self.engine = create_engine(connection_str, echo=False)
        self.conn = connection_str
        self.session = self.create_new_session()  # create default session

    def get_session(self) -> Session:
        """get session from either flask global object or existing singleton session

        Returns:
            [type]: [description]
        """
        from flask import g  # g is the global object in flask

        session = None
        if (
            g
        ):  # as we don't run flask in our pipeline, so g will be None. We use this as a flag to check if the request is coming from web or pipeline
            session = g.get(
                "session", None
            )  # get the request session from flask global object, otherwise it will return None.
            return session
        if session is None:
            return (
                self.session
            )  # Default session will be returned, this will be the default option for pipeline as one session will be created for each instance of pipeline

    def create_new_session(self) -> Session:
        """creates new sqlalchemy session"""
        return sessionmaker(bind=self.engine)()

    def before_flask_request(self):
        """This is part of the flask request cycle, it creates a new database session for every request"""
        from flask import g

        session = self.create_new_session()
        g.session = session  # attach the session to global object so that it can be used in flask request

    def after_flask_request(self):
        """This is part of the flask life cycle, it commit and close the db session at the end of flask request"""
        from flask import g

        session = g.get("session", None)
        if session is not None:
            try:
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()

    def after_flask_request_rollback(self):
        """This is part of the flask life cycle, it rollback(no changes will be persisted in the db) the db session at the end of flask request"""
        from flask import g

        session = g.get("session", None)
        if session is not None:
            session.rollback()

    def bulk_save(self, items: List):
        """It stores all items in one new db session

        Args:
            items (List): List of items
        """
        session = self.create_new_session()
        try:
            session.bulk_save_objects(items)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def session_scope(self):
        """creates a session scope for database operation as decorator by using singleton db session

        Yields:
            [type]: sqlalchemy session
        """
        session = self.get_session()
        """Provide a transactional scope around a series of operations."""
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def new_session_scope(self):
        """creates a session scope for database operation as decorator by creating a new db session

        Yields:
            [type]: sqlalchemy session
        """
        self.session = self.create_new_session()
        session = self.get_session()
        """Provide a transactional scope around a series of operations."""
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self):
        """create tables by using sqlalchemy context"""
        self.db_base.metadata.create_all(self.engine)

    def drop_tables(self):
        """drop tables by using sqlalchemy context"""
        self.db_base.metadata.drop_all(self.engine)


def db_session(func):
    """decorater for wrapping the function into db session
    Args:
        func ([type]): function that will run under db session
    """

    def wrapper(*args, **kwargs):
        with Context().session_scope() as session:
            # kwargs['session'] = session
            func(*args, **kwargs)

    return wrapper
