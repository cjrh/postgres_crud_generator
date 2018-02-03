"""
Postgres CRUD generator
=======================

Code generator to make CRUD classes for an existing Postgres DB.

"""
import asyncio
from collections import OrderedDict
from typing import NamedTuple, List, Dict, Iterable
from textwrap import indent

from asyncpg.connection import Connection
from argparse import (
    RawDescriptionHelpFormatter,
    ArgumentDefaultsHelpFormatter,
    ArgumentParser
)
from string import Template
from pprint import pprint
import datetime


import asyncpg
import ipaddress
import datetime
from decimal import Decimal
import uuid


__version__ = '0.0.0'


header = f'''\
""" DO NOT MODIFY!!!

 Autogenerated at {datetime.datetime.now().ctime()}

"""
from collections import OrderedDict
import uuid
import datetime
from decimal import Decimal
import ipaddress
import json
from typing import List, Sequence
from pprint import pformat
import asyncpg
from asyncpg.pool import Pool


pool: Pool = None


class UNCHANGED:
    pass
    
    
class CRUDTable:
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        return self.json(pretty=True)
        
    def json(self, pretty=None) -> str:
        return json.dumps(
            self.__dict__,
            default=str,
            indent=pretty and 4
        )
        
    @classmethod
    def from_json(cls, text: str):
        # TODO: this doesn't really work. Need to be able to deserialise
        # TODO: arbitrary types back from str.
        return cls(**json.loads(text))
    
'''


postgres_to_python = {
    'anyarray': list,
    'anyenum': str,
    'anyrange': asyncpg.Range,
    'record': asyncpg.Record,  #: tuple,
    'bit': asyncpg.BitString,
    'varbit': asyncpg.BitString,
    'bool': bool,
    'box': asyncpg.Box,
    'bytea': bytes,
    'char': str,
    'name': str,
    'varchar': str,
    'text': str,
    'xml': str,
    'cidr': ipaddress.IPv4Network,
    'inet': ipaddress.IPv4Network,
    'macaddr': str,
    'circle': asyncpg.Circle,
    'date': datetime.date,
    'time': datetime.time,
    'time with timezone': datetime.time,
    'time without timezone': datetime.time,
    'timestamp': datetime.datetime,
    'timestamp with timezone': datetime.datetime,
    'timestamp without timezone': datetime.datetime,
    'interval': datetime.timedelta,
    'float': float,
    'double precision': float,
    'smallint': int,
    'integer': int,
    'bigint': int,
    'numeric': Decimal,
    'json': str,
    'jsonb': str,
    'line': asyncpg.Line,
    'lseg': asyncpg.LineSegment,
    'money': str,
    'path': asyncpg.Path,
    'point': asyncpg.Point,
    'polygon': asyncpg.Polygon,
    'uuid': uuid.UUID,
}


postgres_to_python_text = {
    'anyarray': 'list',
    'anyenum': 'str',
    'anyrange': 'asyncpg.Range',
    'int4range': 'asyncpg.Range',
    'record': 'asyncpg.Record,  #: tuple',
    'bit': 'asyncpg.BitString',
    'varbit': 'asyncpg.BitString',
    'bool': 'bool',
    'boolean': 'bool',
    'box': 'asyncpg.Box',
    'bytea': 'bytes',
    'char': 'str',
    'name': 'str',
    'varchar': 'str',
    'text': 'str',
    'xml': 'str',
    'cidr': 'ipaddress.IPv4Network',
    'inet': 'ipaddress.IPv4Network',
    'macaddr': 'str',
    'circle': 'asyncpg.Circle',
    'date': 'datetime.date',
    'time': 'datetime.time',
    'time with timezone': 'datetime.time',
    'time without time zone': 'datetime.time',
    'timestamp': 'datetime.datetime',
    'timestamp with timezone': 'datetime.datetime',
    'timestamp without time zone': 'datetime.datetime',
    'interval': 'datetime.timedelta',
    'float': 'float',
    'double precision': 'float',
    'smallint': 'int',
    'integer': 'int',
    'bigint': 'int',
    'numeric': 'Decimal',
    'json': 'str',
    'jsonb': 'str',
    'line': 'asyncpg.Line',
    'lseg': 'asyncpg.LineSegment',
    'money': 'str',
    'path': 'asyncpg.Path',
    'point': 'asyncpg.Point',
    'polygon': 'asyncpg.Polygon',
    'uuid': 'uuid.UUID',
}


TABLE_TEMPLATE = Template('''\
#
# noinspection PyShadowingBuiltins,PyPep8Naming
class ${table_name}(CRUDTable):
    def __init__(self, *, $init_params):
        """ This initializer is mainly for static type engines. Use one
        of the classmethods to create instances (with DB interaction) """
$init_assignments

    @classmethod
    async def create(cls, *, $create_params) -> '${table_name}':
        async with pool.acquire() as conn:
            r = await conn.fetchrow("""\\
                INSERT INTO slayer2.${table_name} 
                    ($calc_fields)
                VALUES (
                    $field_nums
                ) RETURNING *
            """, $calc_fields)
            return cls(**r)

    @classmethod
    async def read(cls, *, $pk: $pktype) -> '${table_name}':
        async with pool.acquire() as conn:
            records = await conn.fetch("""\\
                SELECT * FROM slayer2.${table_name}
                WHERE
                    ${table_name}.${pk} = $1
            """, $pk)

            return cls(**records[0])

    @classmethod
    async def read_many(cls, where_clause: str = '', params: Sequence = ()) -> 'List[${table_name}]':
        final_where = ''
        if where_clause:
            final_where = f'\\nWHERE {where_clause}'
        
        async with pool.acquire() as conn:
            records = await conn.fetch(f"""\\
                SELECT * FROM slayer2.${table_name}{final_where}
            """, *params)

            return [cls(**r) for r in records]
            
    async def update(self, *, $update_params):
        sql_fields = []
        sql_values = []
        fieldnames = [$update_fieldnames]
        fieldvalues = [$update_fieldvalues]
        changed_fieldnames = []
        changed_fieldvalues = []
        count = 2
        for f, v in zip(fieldnames, fieldvalues):
            if v is UNCHANGED:
                continue

            changed_fieldnames.append(f)
            changed_fieldvalues.append(v)

            sql_fields.append(f'{f} = ${count}')
            sql_values.append(v)
            count += 1

        async with pool.acquire() as conn:
            await conn.execute(f"""\\
                UPDATE ${table_name}
                SET
                    {', '.join(sql_fields)}
                WHERE
                    ${table_name}.${pk} = $1
            """, self.$pk, *sql_values)

        for f, v in zip(changed_fieldnames, changed_fieldvalues):
            setattr(self, f, v)

    @classmethod
    async def update_many(cls, *, where_clause: str = '', where_params: Sequence = (), $update_params):
        """ Parameters in the `where_clause` will start numbering at $1."""
        final_where = ''
        if where_clause:
            final_where = f'\\nWHERE {where_clause}'
        
        sql_fields = []
        sql_values = []
        fieldnames = [$update_fieldnames]
        fieldvalues = [$update_fieldvalues]
        count = 1 + len(where_params)
        for f, v in zip(fieldnames, fieldvalues):
            if v is UNCHANGED:
                continue

            sql_fields.append(f'{f} = ${count}')
            sql_values.append(v)
            count += 1

        async with pool.acquire() as conn:
            await conn.execute(f"""\\
                UPDATE ${table_name}
                SET
                    {', '.join(sql_fields)}
                WHERE
                    {where_clause}
            """, *where_params, *sql_values)
            
    async def delete(self):
        async with pool.acquire() as conn:
            deleted_at = datetime.datetime.utcnow()
            await conn.execute(f"""\\
                UPDATE slayer2.${table_name}
                SET deleted_at = $2
                WHERE
                    ${pk} = $1
            """, self.$pk, deleted_at) 
        self.deleted_at = deleted_at

    async def delete_hard(self):
        async with pool.acquire() as conn:
            await conn.execute(f"""\\
                DELETE FROM slayer2.${table_name}
                WHERE
                    ${table_name}.${pk} = $1
            """, self.$pk)
''')


class Column(NamedTuple):
    name: str  # column_name
    type: str  # data_type
    default: str = ''  # column_default
    nullstr: str = ''  # or it could be " = None"


class Table(NamedTuple):
    name: str  # table_name
    columns: Dict[str, Column]


def comma_sep(columns: Iterable[Column], wrap='', filter=lambda c: True) -> str:
    return ', '.join(f'{wrap}{c.name}{wrap}' for c in columns if filter(c))


def comma_sep_type(columns: Iterable[Column], filter=lambda c: True) -> str:
    return ', '.join(f'{c.name}: {c.type}' for c in columns if filter(c))


def comma_sep_type_none(columns: Iterable[Column], filter=lambda c: True) -> str:
    return ', '.join(f'{c.name}: {c.type}{c.nullstr}' for c in columns if filter(c))


def comma_sep_type_def(columns: Iterable[Column], default=' = UNCHANGED', filter=lambda c: True) -> str:
    return ', '.join(f'{c.name}: {c.type}{default}' for c in columns if filter(c))


async def generate(conn: Connection, args):
    table_skips = {'alembic_version'}
    pkeys = await get_primary_keys(conn)
    pprint(pkeys)

    columns = await conn.fetch('''\
        select *
        from slayer2.information_schema.columns
        where table_schema = 'slayer2';
    ''')

    tables: Dict[str, Table] = {}

    for c in columns:
        print(c)
        table_name = c['table_name']
        if table_name not in tables:
            tables[table_name] = Table(
                name=table_name,
                columns=OrderedDict()
            )

        colname = c['column_name']
        tables[table_name].columns[colname] = Column(
            name=colname,
            type=postgres_to_python_text.get(c['data_type'], f"'{c['data_type']}'"),
            default=c['column_default'],
            nullstr=' = None' if c['is_nullable'] == 'YES' else ''
        )

    pprint(tables)

    # Generate output
    generated_classes = [header]

    for table_name, table in tables.items():
        if table_name in table_skips:
            continue

        all_fields = []
        all_fields_types = []

        cu_fields = []
        cu_fields_types = []
        cu_args = []
        cu_defaults = []

        for cn, c in table.columns.items():
            all_fields.append(c.name)
            all_fields_types.append(c.type)

            if c.default:
                continue

            cu_fields.append(c.name)
            cu_fields_types.append(c.type)
            cu_args.append(f'{c.name}: {c.type} = UNCHANGED')
            # cu_defaults.append(
            #     f' = None' if c.is_nullable else ''
            # )


            # if c.is_nullable:
            #     default_none = ' = None'
            # else:
            #     default_none = ''
            #     init_assignment = ' = {c.name}'


        out = TABLE_TEMPLATE.safe_substitute(
            table_name=table_name,

            # init method
            init_params=comma_sep_type_none(
                table.columns.values()
            ),
            init_assignments=indent(
                '\n'.join(f'self.{f} = {f}' for f in all_fields), ' '*4*2
            ),

            # create
            create_params=comma_sep_type_none(
                table.columns.values(),
                filter=lambda c: not c.default
            ),

            # update
            update_params=comma_sep_type_def(
                table.columns.values(),
                filter=lambda c: not c.default
            ),
            update_fieldnames=comma_sep(
                table.columns.values(),
                wrap="'",
                filter=lambda c: not c.default
            ),
            update_fieldvalues=comma_sep(
                table.columns.values(),
                filter=lambda c: not c.default
            ),

            # Other
            field_nums=', '.join(f'${i + 1}' for i in range(len(cu_fields))),
            calc_fields=', '.join(cu_fields),

            pk=pkeys[table_name],
            pktype=tables[table_name].columns[pkeys[table_name]].type,
        )
        print()
        print(out)
        generated_classes.append(out)
        print()

    with open(args.outfile, 'w') as f:
        f.write('\n\n'.join(generated_classes))


    # await conn.execute('''
    #     CREATE TABLE users(
    #         id serial PRIMARY KEY,
    #         name text,
    #         dob date
    #     )
    # ''')
    #
    # # Insert a record into the created table.
    # await conn.execute('''
    #     INSERT INTO users(name, dob) VALUES($1, $2)
    # ''', 'Bob', datetime.date(1984, 3, 1))
    #
    # # Select a row from the table.
    # row = await conn.fetchrow(
    #     'SELECT * FROM users WHERE name = $1', 'Bob')
    # # *row* now contains
    # # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))


async def main(args):
    # dsn = 'postgresql://slayer2:slayer2@localhost:41329/slayer2'
    dsn = f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}'
    conn: Connection = await asyncpg.connect(dsn)
    try:
        await generate(conn, args)
    finally:
        await conn.close()


async def get_primary_keys(conn: Connection) -> Dict[str, str]:
    sql = '''\
    SELECT  *
    FROM    slayer2.INFORMATION_SCHEMA.TABLES t
             LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                     ON tc.table_catalog = t.table_catalog
                     AND tc.table_schema = t.table_schema
                     AND tc.table_name = t.table_name
                     AND tc.constraint_type = 'PRIMARY KEY'
             LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                     ON kcu.table_catalog = tc.table_catalog
                     AND kcu.table_schema = tc.table_schema
                     AND kcu.table_name = tc.table_name
                     AND kcu.constraint_name = tc.constraint_name
    WHERE   t.table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY t.table_catalog,
             t.table_schema,
             t.table_name,
             kcu.constraint_name,
             kcu.ordinal_position;
    '''

    records = await conn.fetch(sql)
    out = {}
    for r in records:
        out[r['table_name']] = r['column_name']
    return out


def entrypoint():
    class Formatter(RawDescriptionHelpFormatter, ArgumentDefaultsHelpFormatter):
        pass

    parser = ArgumentParser(
        description=__doc__,
        formatter_class=Formatter
    )
    parser.add_argument(
        '--db', type=str, help='Database name')
    parser.add_argument(
        '--user', type=str, default='postgres',
        help='Username for database connection.')
    parser.add_argument(
        '--password', type=str, default='postgres',
        help='Password for database connection.')
    parser.add_argument(
        '--port', type=int, default=5432,
        help='Database port'
    )
    parser.add_argument(
        '--host', type=str, default='localhost',
        help='Hostname for the database'
    )
    parser.add_argument(
        '-o', '--outfile', default='generated.py',
        help='Name of the generated file.'

    )
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    entrypoint()
