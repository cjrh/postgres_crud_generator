import logging
import asyncio
from random import choice
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


async def reads():
    all_allocations = await generated.allocated_port.read_many('1=1')
    random_allocation: generated.allocated_port = choice(all_allocations)

    logging.getLogger('asyncpg').setLevel('DEBUG')

    pa: generated.allocated_port = await generated.allocated_port.read(
        id=random_allocation.id
    )

    print()
    print(pa)

    port = await generated.port.read(id=pa.port_id)
    print(port)

    device = await generated.device.read(id=port.device_id)
    print(device)

    pop = await generated.pop.read(id=device.pop_id)
    print(pop)

    region = await generated.region.read(id=pop.region_id)
    print(region)


def test_read(poolinit):
    loop.run_until_complete(reads())


async def create_stuff():
    # Choose device
    devices = await generated.device.read_many()

    vlan: generated.vlan = await generated.vlan.create(
        label=str(uuid4()),
        vid=1234,
        device_id=choice(devices).id
    )
    print(vlan)

    fetch_vlan: generated.vlan = await generated.vlan.read(id=vlan.id)
    print('after fetch:', fetch_vlan)

    assert vlan.id == fetch_vlan.id


def test_create(poolinit):
    loop.run_until_complete(create_stuff())


async def update_stuff():
    devices = await generated.device.read_many()
    vlan: generated.vlan = await generated.vlan.create(
        label=str(uuid4()),
        vid=1235,
        device_id=choice(devices).id
    )
    print(vlan)

    fetch_vlan: generated.vlan = await generated.vlan.read(id=vlan.id)
    print('after fetch:', fetch_vlan)
    await fetch_vlan.update(deleted_at=datetime.datetime.utcnow())
    print('after delete, before fetch:', fetch_vlan)
    assert fetch_vlan.deleted_at is not None

    fetch_vlan: generated.vlan = await generated.vlan.read(id=vlan.id)
    print('after delete, after fetch:', fetch_vlan)
    assert fetch_vlan.deleted_at is not None


def test_update(poolinit):
    loop.run_until_complete(update_stuff())


async def delete_stuff():
    devices = await generated.device.read_many()
    vlan: generated.vlan = await generated.vlan.create(
        label=str(uuid4()),
        vid=1236,
        device_id=choice(devices).id
    )
    print(vlan)
    await vlan.delete()
    print('after delete, before fetch:', vlan)
    assert vlan.deleted_at is not None

    fetch_vlan: generated.vlan = await generated.vlan.read(id=vlan.id)
    print('after delete, after fetch:', fetch_vlan)
    assert fetch_vlan.deleted_at is not None


def test_delete(poolinit):
    loop.run_until_complete(delete_stuff())


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


async def enum_stuff():
    devices = await generated.device.read_many()
    device_id = choice(devices).id
    item = await generated.reserved_vid.create(
        vid=asyncpg.Range(lower=10, upper=20),
        type=generated.vid_reservation_enum.Customer.value,
        device_id=device_id
    )
    print(item)


def test_enum(poolinit):
    loop.run_until_complete(enum_stuff())


async def json_stuff():
    devices = await generated.device.read_many()
    device_id = choice(devices).id
    item = await generated.reserved_vid.create(
        vid=asyncpg.Range(lower=10, upper=20),
        type=generated.vid_reservation_enum.Customer.value,
        device_id=device_id
    )
    serialized = item.json(pretty=True)
    assert serialized
    print(serialized)


def test_json(poolinit):
    loop.run_until_complete(json_stuff())
