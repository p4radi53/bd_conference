from dataclasses import dataclass

from spark_common.jdbc.jdbc_reader import DbConf
from spark_common.tables.table import TableDelta


@dataclass
class Tables:
    easy_access_zone: TableDelta


@dataclass
class Databases:
    dwh: DbConf


@dataclass
class Config:
    databases: Databases
    tables: Tables
