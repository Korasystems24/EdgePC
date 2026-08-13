import sqlalchemy.orm
from sqlalchemy.orm import mapped_column


class IntIdPkMixin:
    id: sqlalchemy.orm.Mapped[int] = mapped_column(primary_key=True)
