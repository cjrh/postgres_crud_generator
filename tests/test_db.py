from typing import List
from uuid import uuid4

import datetime
import pytest
import asyncpg


def test_blah(db_connection: asyncpg.pool.Pool, loop):
    print(db_connection)

    async def make_table():
        await db_connection.execute('''
            CREATE TABLE IF NOT EXISTS demo(
                id serial PRIMARY KEY, 
                sample_uuid uuid,
                sample_ts timestamp,
                sample_tsz timestamptz,
                sample_text text,
                sample_int INTEGER,
                sample_double DOUBLE PRECISION
            )
        ''')

    loop.run_until_complete(make_table())

    async def make_data():
        await db_connection.execute(f'''\
            INSERT INTO demo (
                sample_uuid, 
                sample_ts,
                sample_tsz,
                sample_text,
                sample_int,
                sample_double
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ''',
            uuid4(),
            datetime.datetime.now(),
            datetime.datetime.now(tz=datetime.timezone.utc),
            'blah',
            100,
            1.1
            )

    loop.run_until_complete(make_data())

    async def fetch_data():
        records = await db_connection.fetch('''\
            SELECT * FROM demo
        ''')
        return records

    records: List[asyncpg.Record] = loop.run_until_complete(fetch_data())
    print(records)
