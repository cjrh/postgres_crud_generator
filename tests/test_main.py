import asyncio
from uuid import uuid4

import asyncpg
import pytest
import datetime

import generated


loop = asyncio.get_event_loop()


@pytest.fixture(scope='session')
def poolinit():
    dsn = 'postgresql://slayer2:slayer2@localhost:41329/slayer2'

    async def get_pool():
        return await asyncpg.create_pool(dsn=dsn)

    pool = loop.run_until_complete(get_pool())
    generated.pool = pool


def test_basic(poolinit):

    async def ex():
        async with generated.pool.acquire() as conn:
            return await conn.fetch('select * from allocated_port')

    x = loop.run_until_complete(ex())

    print('x=', x)


def test_read(poolinit):

    pa: generated.allocated_port = loop.run_until_complete(
        generated.allocated_port.read(id='8aabca54-fed2-408c-b0bf-dac9722707e0')
    )
    print()
    print(pa)

    port = loop.run_until_complete(
        generated.port.read(id=pa.port_id)
    )
    print(port)

    device = loop.run_until_complete(
        generated.device.read(id=port.device_id)
    )
    print(device)

    pop = loop.run_until_complete(
        generated.pop.read(id=device.pop_id)
    )
    print(pop)

    region = loop.run_until_complete(
        generated.region.read(id=pop.region_id)
    )
    print(region)


def test_create(poolinit):

    vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.create(
            label=uuid4()
        )
    )

    print(vlan)

    fetch_vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.read(id=vlan.id)
    )

    print('after fetch:', fetch_vlan)


def test_update(poolinit):
    vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.create(
            label=uuid4()
        )
    )
    print(vlan)

    fetch_vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.read(id=vlan.id)
    )
    print('after fetch:', fetch_vlan)

    loop.run_until_complete(
        fetch_vlan.update(deleted_at=datetime.datetime.utcnow())
    )
    print('after delete, before fetch:', fetch_vlan)
    assert fetch_vlan.deleted_at is not None

    fetch_vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.read(id=vlan.id)
    )
    print('after delete, after fetch:', fetch_vlan)
    assert fetch_vlan.deleted_at is not None


def test_delete(poolinit):
    vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.create(
            label=uuid4()
        )
    )
    print(vlan)

    loop.run_until_complete(vlan.delete())
    print('after delete, before fetch:', vlan)
    assert vlan.deleted_at is not None

    fetch_vlan: generated.vlan = loop.run_until_complete(
        generated.vlan.read(id=vlan.id)
    )
    print('after delete, after fetch:', fetch_vlan)
    assert fetch_vlan.deleted_at is not None


def test_read_many(poolinit):

    items = loop.run_until_complete(
        generated.vlan.read_many('1=1', [])
    )

    print()
    print(items)

    len0 = len(items)

    vlan = items[-1]

    loop.run_until_complete(
        vlan.delete_hard()
    )

    items = loop.run_until_complete(
        generated.vlan.read_many('1=1', [])
    )
    assert len(items) == len0 - 1


def test_read_many_filter(poolinit):

    items = loop.run_until_complete(
        generated.vlan.read_many(
            'deleted_at is not null',
            []
        )
    )

    print()
    print(len(items), items)

