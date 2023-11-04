import random

from faker import Faker
from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Teacher, Student, Group, Grade, Subject

fake = Faker('uk-UA')


def insert_groups():
    for i in range(3):
        group = Group(
            name=f'Group {chr(65 + i)}'
        )
        session.add(group)


def insert_teachers():
    for _ in range(5):
        teacher = Teacher(
            full_name=fake.name()
        )
        session.add(teacher)


def insert_students():
    for _ in range(40):
        student = Student(
            full_name=fake.name(),
            group_id=random.randint(1, 3)
        )
        session.add(student)


def insert_subjects():
    for _ in range(8):
        subject = Subject(
            name=fake.job(),
            teacher_id=random.randint(1, 3)
        )
        session.add(subject)


def insert_grades():
    for student_id in range(1, 41):
        for subject_id in range(1, 9):
            grade = Grade(
                student_id=student_id,
                subject_id=subject_id,
                grade=random.randint(60, 100),
                date_received=fake.date_between(start_date='-1y', end_date='today')
            )
            session.add(grade)


if __name__ == '__main__':
    try:
        insert_groups()
        insert_teachers()
        insert_students()
        insert_subjects()
        insert_grades()
        session.commit()
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()
