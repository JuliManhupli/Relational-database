from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Group(Base):
    __tablename__ = 'groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class Teacher(Base):
    __tablename__ = 'teachers'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class Student(Base):
    __tablename__ = 'students'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(255), nullable=False)
    group_id = Column(Integer, ForeignKey('groups.id', ondelete='CASCADE'))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    group = relationship('Group', backref='students')


class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    teacher_id = Column(Integer, ForeignKey('teachers.id', ondelete='CASCADE'))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    teacher = relationship('Teacher', backref='subjects')


class Grade(Base):
    __tablename__ = 'grades'

    id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey('students.id', ondelete='CASCADE'))
    subject_id = Column(Integer, ForeignKey('subjects.id', ondelete='CASCADE'))
    grade = Column(Integer)
    date_received = Column(Date, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    student = relationship('Student', backref='grades')
    subject = relationship('Subject', backref='grades')
