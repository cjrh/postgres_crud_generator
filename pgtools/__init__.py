from collections import defaultdict
from typing import List, NamedTuple, Dict
from dataclasses import dataclass

from asyncpg import Connection


class ForeignKey(NamedTuple):
    constraint_name: str
    source_schema: str
    source_table: str
    source_column: str
    target_schema: str
    target_table: str
    target_column: str


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