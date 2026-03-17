"""Database session factory — singleton pattern for SQLAlchemy engine and sessions."""

import logging
from collections.abc import Generator
from contextlib import contextmanager

from config.settings import get_settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)


class SessionFactory:
    """Singleton factory that creates and manages SQLAlchemy sessions.

    Usage:
        factory = SessionFactory()
        with factory.session() as session:
            session.query(...)
    """

    _instance: "SessionFactory | None" = None
    _engine: Engine | None = None
    _session_maker: sessionmaker | None = None

    def __new__(cls) -> "SessionFactory":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _initialize(self) -> None:
        """Create engine and session maker on first use."""
        if self._engine is None:
            settings = get_settings()
            self._engine = create_engine(
                settings.database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
            )
            self._session_maker = sessionmaker(self._engine)
            logger.info("Database engine initialized", extra={"url": settings.database_url.split("@")[-1]})

    @property
    def engine(self) -> Engine:
        """Return the SQLAlchemy engine, initializing if needed."""
        self._initialize()
        if self._engine is None:
            raise RuntimeError("Database engine failed to initialize")
        return self._engine

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Provide a transactional session scope.

        Commits on success, rolls back on exception, always closes.
        """
        self._initialize()
        if self._session_maker is None:
            raise RuntimeError("Session maker failed to initialize")
        session = self._session_maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton — useful for testing."""
        if cls._engine is not None:
            cls._engine.dispose()
        cls._engine = None
        cls._session_maker = None
        cls._instance = None
