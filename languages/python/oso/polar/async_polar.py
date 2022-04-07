"""Communicate with the Polar virtual machine: load rules, make queries, etc."""

from datetime import datetime, timedelta
import os
from pathlib import Path
import sys
from typing import Dict, List, Union

from .exceptions import (
    PolarRuntimeError,
    InlineQueryFailedError,
    ParserError,
    PolarFileExtensionError,
    PolarFileNotFoundError,
    InvalidQueryTypeError,
)
from .ffi import Polar as FfiPolar, PolarSource as Source
from .host import Host
from .polar import Polar
from .async_query import AsyncQuery
from .predicate import Predicate
from .variable import Variable
from .expression import Expression, Pattern
from .data_filtering import serialize_types, filter_data
from .data import DataFilter

CLASSES: Dict[str, type] = {}


class AsyncPolar(Polar):
    async def load_files(self, filenames: List[Union[Path, str]] = []):
        """Load Polar policy from ".polar" files."""
        if not filenames:
            return

        sources: List[Source] = []

        for filename in filenames:
            path = Path(filename)
            extension = path.suffix
            filename = str(path)
            if not extension == ".polar":
                raise PolarFileExtensionError(filename)

            try:
                with open(filename, "rb") as f:
                    src = f.read().decode("utf-8")
                    sources.append(Source(src, filename))
            except FileNotFoundError:
                raise PolarFileNotFoundError(filename)

        await self._load_sources(sources)

    # Register MROs, load Polar code, and check inline queries.
    async def _load_sources(self, sources: List[Source]):
        self.host.register_mros()
        self.ffi_polar.load(sources)
        await self.check_inline_queries()

    async def new_authorized_query(self, actor, action, resource_cls):
        results = await self.partial_query(actor, action, resource_cls)

        types = serialize_types(self.host.distinct_user_types(), self.host.types)
        class_name = self.host.types[resource_cls].name
        plan = self.ffi_polar.build_data_filter(types, results, "resource", class_name)

        return self.host.adapter.build_query(DataFilter.parse(self, plan))

    async def partial_query(self, actor, action, resource_cls):
        resource = Variable("resource")
        class_name = self.host.types[resource_cls].name
        constraint = Expression(
            "And", [Expression("Isa", [resource, Pattern(class_name, {})])]
        )

        query = await self.query_rule(
            "allow",
            actor,
            action,
            resource,
            bindings={"resource": constraint},
            accept_expression=True,
        )

        return [
            {"bindings": {k: self.host.to_polar(v)}}
            async for result in query
            for k, v in result["bindings"].items()
        ]


    async def check_inline_queries(self):
        while True:
            query = self.ffi_polar.next_inline_query()
            if query is None:  # Load is done
                break
            else:
                try:
                    next(AsyncQuery(query, host=self.host.copy()).run())
                except StopIteration:
                    source = query.source()
                    raise InlineQueryFailedError(source)

    async def query_rule(self, name, *args, **kwargs):
        """Query for rule with name ``name`` and arguments ``args``.

        :param name: The name of the predicate to query.
        :param args: Arguments for the predicate.

        :return: The result of the query.
        """
        return self.query(Predicate(name=name, args=args), **kwargs)


    async def query(self, query, *, bindings=None, accept_expression=False):
        """Query for a predicate, parsing it if necessary.

        :param query: The predicate to query for.

        :return: The result of the query.
        """
        host = self.host.copy()
        host.set_accept_expression(accept_expression)

        if isinstance(query, str):
            query = self.ffi_polar.new_query_from_str(query)
        elif isinstance(query, Predicate):
            query = self.ffi_polar.new_query_from_term(host.to_polar(query))
        else:
            raise InvalidQueryTypeError()

        q = AsyncQuery(query, host=host, bindings=bindings).run()
        async for res in q:
            print("xxxxxxx")
            print(res)
            print("/xxxxxxx")
            yield res
