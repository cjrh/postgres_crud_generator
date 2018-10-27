from __future__ import annotations
from collections import defaultdict
from typing import List, NamedTuple, Dict
from dataclasses import dataclass

from asyncpg import Connection


@dataclass
class ForeignKey:
    constraint_name: str
    source_schema: str
    source_table: str
    source_column: str
    target_schema: str
    target_table: str
    target_column: str


@dataclass
class Col:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    ordinal_position: int
    column_default: str
    is_nullable: str
    data_type: str
    udt_catalog: str
    udt_schema: str
    udt_name: str
    is_updatable: str

    @classmethod
    async def fetch(cls, connection: Connection,
                    db_name: str,
                    schema_name: str = 'public') -> List[Col]:
        records = await connection.fetch(f'''\
            select
                table_catalog,
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                column_default,
                is_nullable,
                data_type,
                udt_catalog,
                udt_schema,
                udt_name,
                is_updatable
            from {db_name}.information_schema.columns
            where table_schema = '{schema_name}';
        ''')

        return [Col(**r) for r in records]


@dataclass
class Column:
    name: str  # column_name
    type: str  # data_type
    default: str = ''  # column_default
    nullstr: str = ''  # or it could be " = None"
    comment: str = ''
    is_primary_key: bool = False
    fkey: ForeignKey = None


@dataclass
class Table:
    name: str  # table_name
    columns: Dict[str, Column]


async def get_fk_data(connection: Connection) -> List[ForeignKey]:
    sql = '''\
    SELECT
      o.conname AS constraint_name,
      (SELECT nspname FROM pg_namespace WHERE oid=m.relnamespace) AS source_schema,
      m.relname AS source_table,
      (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = m.oid AND a.attnum = o.conkey[1] AND a.attisdropped = false) AS source_column,
      (SELECT nspname FROM pg_namespace WHERE oid=f.relnamespace) AS target_schema,
      f.relname AS target_table,
      (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = f.oid AND a.attnum = o.confkey[1] AND a.attisdropped = false) AS target_column
    FROM
      pg_constraint o LEFT JOIN pg_class c ON c.oid = o.conrelid
      LEFT JOIN pg_class f ON f.oid = o.confrelid LEFT JOIN pg_class m ON m.oid = o.conrelid
    WHERE
      o.contype = 'f' AND o.conrelid IN (SELECT oid FROM pg_class c WHERE c.relkind = 'r');
    '''
    records = await connection.fetch(sql)
    results = []
    for r in records:
        results.append(
            ForeignKey(
                constraint_name=r['constraint_name'],
                source_schema=r['source_schema'],
                source_table=r['source_table'],
                source_column=r['source_column'],
                target_schema=r['target_schema'],
                target_table=r['target_table'],
                target_column=r['target_column']
            )
        )
    return results


async def get_enums(conn: Connection) -> Dict[str, List[str]]:
    """ Obtain all the custom enums """
    sql = '''\
    select
      pg_type.typname,
      pg_enum.enumlabel
    from pg_type
    join pg_enum on pg_enum.enumtypid = pg_type.OID;
    '''
    records = await conn.fetch(sql)
    result = defaultdict(list)
    for r in records:
        result[r['typname']].append(r['enumlabel'])
    return result


async def get_primary_keys(conn: Connection, db_name: str) -> Dict[str, str]:
    sql = f'''\
    SELECT  *
    FROM    {db_name}.INFORMATION_SCHEMA.TABLES t
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