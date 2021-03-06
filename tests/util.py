# util.py
import logging
import argparse, asyncio, asyncpg
from asyncpg.pool import Pool

DSN = 'postgresql://{user}:{password}@{host}:{port}'
DSN_DB = DSN + '/{name}'
CREATE_DB = 'CREATE DATABASE {name};'
DROP_DB = 'DROP DATABASE {name};'


apglog = logging.getLogger('asyncpg')
apglog.setLevel('DEBUG')


class Database:
    def __init__(self, name, owner=False, host='localhost',
                 port=5432, user='postgres', password='postgres',
                 **kwargs):
        """If ``owner`` is True, then ``connect()`` will also first
        create the database, and ``disconnect`` will also drop it."""
        self.params = dict(
            user=user,
            password=password,
            host=host,
            port=port,
            name=name
        )
        self.params.update(kwargs)
        self.pool: Pool = None
        self.owner = owner
        self.listeners = []

    async def connect(self) -> Pool:
        if self.owner:
            await self.server_command(CREATE_DB.format(**self.params))
            # print(output)

        self.pool = await asyncpg.create_pool(DSN_DB.format(**self.params))
        return self.pool

    async def disconnect(self):
        """Destroy the database"""
        if self.pool:
            releases = [self.pool.release(conn) for conn in self.listeners]
            await asyncio.gather(*releases)
            await self.pool.close()

        if self.owner:
            await self.server_command(DROP_DB.format(**self.params))

    async def __aenter__(self) -> Pool:
        return await self.connect()

    async def __aexit__(self, *exc):
        await self.disconnect()

    async def server_command(self, cmd):
        conn = await asyncpg.connect(DSN.format(**self.params))
        await conn.execute(cmd)
        await conn.close()

    async def add_listener(self, channel, callback):
        # TODO: this is very wasteful. Should use a single connection for multiple listeners.
        conn: asyncpg.Connection = await self.pool.acquire()
        await conn.add_listener(channel, callback)
        self.listeners.append(conn)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cmd', choices=['create', 'drop'])
    parser.add_argument('--name', type=str)
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    d = Database(args.name, owner=True)
    if args.cmd == 'create':
        loop.run_until_complete(d.connect())
    elif args.cmd == 'drop':
        loop.run_until_complete(d.disconnect())
    else:
        parser.print_help()