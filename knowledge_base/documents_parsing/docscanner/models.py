"""
Модели базы данных: файлы и версии для контроля изменений.
"""

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
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
    method = Column(String)  # метод распознавания (pdfplumber, ocr, docx, image_ocr)

    file = relationship(FileRecord, back_populates='versions')
