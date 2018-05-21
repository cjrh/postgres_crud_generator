import logging
import asyncio
import asyncpg
import pytest
import dockerctx
import portpicker
import util
import os


logging.basicConfig(level='DEBUG')


@pytest.fixture
def postgres_port():
    port = os.environ.get('DEVPORT', None) or portpicker.pick_unused_port()
    with dockerctx.new_container(
        image_name='postgres:alpine',
        ports={'5432/tcp': port},
        ready_test=lambda: dockerctx.pg_ready(
            host='localhost',
            port=port,
            timeout=6000
        )
    ) as container:
        yield port


@pytest.fixture(scope='session')
def loop():
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        yield new_loop
    finally:
        new_loop.close()


@pytest.fixture
def db_pool(postgres_port, loop):

    async def dbup():
        db = util.Database(
            name='crudtest',
            owner=True,
            host='localhost',
            port=postgres_port,
            user='postgres',
            password='postgres',
        )
        pool = await db.connect()
        return pool, db

    pool, db = loop.run_until_complete(dbup())
    try:
        yield pool, db.params
    finally:
        loop.run_until_complete(db.disconnect())
