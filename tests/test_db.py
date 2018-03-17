from typing import List, Optional, Tuple
from uuid import uuid4

import datetime
import pytest
import asyncpg
import pg_crud_gen


async def make_table(pool: asyncpg.pool.Pool):
    return await pool.execute(f'''
            CREATE TABLE IF NOT EXISTS demo(
                id SERIAL PRIMARY KEY,
                sample_uuid UUID,
                sample_ts TIMESTAMP,
                sample_tsz TIMESTAMPTZ,
                sample_text TEXT,
                sample_int INTEGER,
                sample_double DOUBLE PRECISION
            )
        ''')


async def make_data(pool: asyncpg.pool.Pool):
    await pool.execute(f'''\
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


def test_simple(db_pool: asyncpg.pool.Pool, loop):
    pool, db_params = db_pool
    print(pool)
    loop.run_until_complete(make_table(pool))
    loop.run_until_complete(make_data(pool))

    async def fetch_data():
        return await pool.fetch('SELECT * FROM demo')

    records: List[asyncpg.Record] = loop.run_until_complete(fetch_data())
    print(records)

    import tempfile

    gen_f = tempfile.NamedTemporaryFile(suffix='.py')
    print(f'Generated filename: {gen_f.name}')
    # gen_f.close()

    from types import SimpleNamespace
    args = SimpleNamespace(outfile=gen_f.name, db='crudtest', **db_params)
    loop.run_until_complete(pg_crud_gen.main(args))

    with open(gen_f.name) as f:
        print('\nGenerated:\n')
        print(f.read())


def run_cli(params: Optional[str] = '') -> Tuple[bool, str]:
    """

    run_cli(
        '--host {host} '
        '--port {port} '
        '--db {name} '
        '--user {user} '
        '--password {password} '
        '-o {output}'.format(output=gen_f.name, **db_params)
    )

    """
    import subprocess as sp
    from pathlib import Path

    executable_path = Path(__file__).parent.parent.joinpath('pg_crud_gen.py')
    out = sp.run(['python', executable_path] + params.split())
    return out.returncode == 0, out.stdout
