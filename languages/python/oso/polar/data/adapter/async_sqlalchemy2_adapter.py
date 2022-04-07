from functools import reduce

import sqlalchemy as sa
from polar.data.adapter import DataAdapter
from polar.data.filter import Projection
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import false, true


class AsyncSqlAlchemyAdapter(DataAdapter):
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
                    [AsyncSqlAlchemyAdapter.sqlize(conj) for conj in conjs],
                    true(),
                )
                for conjs in filter.conditions
            ],
            false(),
        )
        return query.filter(disj).distinct(*self.get_primary_key(filter.model))

    def get_primary_key(self, model_instance):
        model_columns = model_instance.__mapper__.columns
        return [c for c in model_columns if c.primary_key]

    async def execute_query(self, query):
        return (await self.session.execute(query)).scalars().unique().all()

    def sqlize(cond):
        op = cond.cmp
        lhs = AsyncSqlAlchemyAdapter.add_side(cond.left)
        rhs = AsyncSqlAlchemyAdapter.add_side(cond.right)
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
