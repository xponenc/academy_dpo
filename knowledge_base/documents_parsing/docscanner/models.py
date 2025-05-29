"""
Модели базы данных: файлы и версии для контроля изменений.
"""
import json

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import JSONB  # для PostgreSQL
from sqlalchemy.orm import relationship
from sqlalchemy.orm import object_session
from database import Base
import datetime

class FileRecord(Base):
    __tablename__ = 'files'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    url = Column(String, nullable=False)
    path = Column(String, unique=True, nullable=False)  # локальный путь к файлу
    sha256 = Column(String(64), nullable=False)
    size = Column(Integer)
    last_modified = Column(DateTime)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    versions = relationship("FileVersion", back_populates='file', cascade='all, delete-orphan')

    @property
    def latest_version(self):
        session = object_session(self)
        return session.query(FileVersion) \
            .filter_by(file_id=self.id) \
            .order_by(FileVersion.processed_at.desc()) \
            .first()


class FileVersion(Base):
    __tablename__ = 'file_versions'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('files.id'))
    text_path = Column(String, nullable=False)  # путь к файлу с распознанным текстом
    processed_at = Column(DateTime, default=datetime.datetime.utcnow)
    method = Column(String)
    # quality_report = Column(JSONB)  # для PostgreSQL
    quality_report = Column(Text)

    file = relationship(FileRecord, back_populates='versions')

    # Метод для сохранения отчёта
    def set_quality_report(self, report_dict):
        self.quality_report = json.dumps(report_dict, ensure_ascii=False)

    # Метод для получения отчёта
    def get_quality_report(self):
        if self.quality_report:
            return json.loads(self.quality_report)
        return None