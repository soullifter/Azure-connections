from sqlalchemy import JSON, Column, Integer, String
from sqlalchemy.orm import declarative_base

SQLDeclarativeBase = declarative_base()


class SampleModel(SQLDeclarativeBase):
    __tablename__ = "sample_model"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    age = Column(String, nullable=False)

    def __repr__(self):
        return (
            f"SampleModel(id = {self.id}, user_id = {self.name}, action = {self.age})"
        )
