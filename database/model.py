from datetime import datetime

from pydantic import constr
from beanie import Document


# ORM использует имя коллекции как имя класса, поэтому переименовал коллекцию, чтобы нейминг не нарушать
class SampleCollection(Document):
    dt: datetime
    value: int
