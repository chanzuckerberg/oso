from functools import reduce

import sqlalchemy as sa
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import false, true

from ..filter import Projection
from .adapter import DataAdapter


class SqlAlchemyAdapter(DataAdapter):
    def __init__(self, session):
        self.session = session

    def build_query(self, filter):
        types = filter.types

        def re(q, rel):
            typ = types[rel.left]
            rec = typ.fields[rel.name]
            left = typ.cls
            right = types[rec.other_type].cls
            return q.join(
                right, getattr(left, rec.my_field) == getattr(right, rec.other_field)
            )

        query = reduce(re, filter.relations, sa.select(filter.model))
        disj = reduce(
            lambda a, b: a | b,
            [
                reduce(
                    lambda a, b: a & b,
                    [SqlAlchemyAdapter.sqlize(conj) for conj in conjs],
                    true(),
                )
                for conjs in filter.conditions
            ],
            false(),
        )
        # NOTE - we're using postgresql's specific DISTINCT ON for the model's
        # primary key columns here because postgresql supports several different
        # unhashable column data types (ex: json/jsonb) that explode when
        # included by a traditional DISTINCT clause.
        return query.filter(disj).distinct(*self.get_primary_key(filter.model))

    def get_primary_key(self, model_instance):
        model_columns = model_instance.__mapper__.columns
        return [c for c in model_columns if c.primary_key]

    def execute_query(self, query):
        return self.session.execute(query).scalars().all()

    def sqlize(cond):
        op = cond.cmp
        lhs = SqlAlchemyAdapter.add_side(cond.left)
        rhs = SqlAlchemyAdapter.add_side(cond.right)
        if op == "Eq":
            return lhs == rhs
        elif op == "Neq":
            return lhs != rhs
        elif op == "In":
            return lhs in rhs
        elif op == "Nin":
            return lhs not in rhs

    def add_side(side):
        if isinstance(side, Projection):
            source = side.source
            field = side.field or inspect(source).primary_key[0].name
            return getattr(source, field)
        elif inspect(type(side), raiseerr=False) is not None:
            return getattr(side, inspect(type(side)).primary_key[0].name)
        else:
            return side
