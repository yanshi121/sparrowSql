import psycopg2
from dbutils.pooled_db import PooledDB

from sparrowSql.tools import PostgresqlCreateTable, ConditionsBuilder, SelectConditionsBuilder


class Postgresql:
    def __init__(self, host: str, port: int, user: str, passwd: str, db: str = None, max_connections: int = 50):
        """
        SparrowSql for Postgresql
        :param host: 数据库地址
        :param port: 数据库端口
        :param user: 操作用户
        :param passwd: 用户密码
        :param db: 数据库名称
        :param max_connections: 最大连接数
        """
        self._host_ = host
        self._port_ = port
        self._user_ = user
        self._passwd_ = passwd
        self._db_ = db
        self._max_connections_ = max_connections
        self._pool_ = PooledDB(
            creator=psycopg2,
            maxconnections=self._max_connections_,
            **{
                'host': self._host_,
                'port': self._port_,
                'user': self._user_,
                'password': self._passwd_,
                'database': self._db_
            }
        )

    def connect_information(self):
        """
        返回数据库连接信息
        :return:
        """
        return {
            "host": self._host_,
            "port": self._port_,
            "user": self._user_,
            "passwd": self._passwd_,
            "db": self._db_
        }

    def user_defined_sql(self, sql: str, params: tuple = None):
        """
        运行自定义SQL\n
        在不输入参数: user_defined_sql('select name from user where id = 1')\n
        输入参数: user_defined_sql('select name from user where id = %s', (1))
        :param sql: SQL
        :param params: 参数，输入参数为参数化查询
        :return:
        """
        connect = self._pool_.connection()
        cursor = connect.cursor()
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def insert(self, table: str, columns: list, values: list):
        """
        插入数据
        :param table: 表名
        :param columns: 字段
        :param values: 插入数据
        :return:
        """
        if type(columns) != list:
            raise Exception(f"columns {columns} type is not list")
        if type(values) != list:
            raise Exception(f"values {columns} type is not list")
        column = "(" + ", ".join(columns) + ")"
        if type(values[0]) is list:
            params = ()
            value_list = []
            for value in values:
                if len(value) == len(columns):
                    for value_s in value:
                        params += (value_s,)
                    value_list.append("(" + ", ".join(["%s" for i in value]) + ")")
                else:
                    raise Exception(f"{columns}->{len(columns)} != {value}->{len(value)}")
            values = ", ".join(value_list)
            sql = f"insert into {table} {column} values {values};"
            connect = self._pool_.connection()
            cursor = connect.cursor()
            cursor.execute(sql, params)
            connect.commit()
            cursor.close()
            connect.close()
            return sql
        else:
            params = ()
            if len(values) == len(columns):
                for value_s in values:
                    params += (value_s,)
                values = "(" + ", ".join(["%s" for i in values]) + ")"
                sql = f"insert into {table} {column} values {values};"
                connect = self._pool_.connection()
                cursor = connect.cursor()
                cursor.execute(sql, params)
                connect.commit()
                cursor.close()
                connect.close()
                return sql
            else:
                raise Exception(f"{columns}->{len(columns)} != {values}->{len(values)}")

    def update(self, table: str, columns_values: dict):
        """
        更新数据
        :param table: 表名
        :param columns_values: 修改的数据
        :return:
        """
        if type(columns_values) is not dict:
            raise TypeError(f"columns_values {columns_values} type is not dict")
        cvs = ', '.join([f"{k}='{v}'" for k, v in columns_values.items()])
        head_sql = f"UPDATE {table} SET {cvs} "
        connect = self._pool_.connection()
        cursor = connect.cursor()
        return ConditionsBuilder(head_sql, cursor, connect)

    def delete(self, table: str):
        """
        删除数据
        :param table: 表名
        :return:
        """
        head_sql = f"DELETE FROM {table}"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        return ConditionsBuilder(head_sql, cursor, connect)

    def select(self, table: str, columns: list = None):
        """
        查询数据
        :param table: 表名
        :param columns: 字段名，默认为全部
        :return:
        """
        if columns is not None and type(columns) is not list:
            raise TypeError(f"columns {columns} type is not dict")
        if columns is None:
            columns_str = "*"
        else:
            columns_str = ", ".join(columns)
        head_sql = f"SELECT {columns_str} FROM {table}"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        return SelectConditionsBuilder(head_sql, cursor, connect)

    def create_table(self, table_name, table_comment=None):
        """
        创建表\n
        table = create_table("test")\n
        table.column('id').type('int').length(10)\n
        table.column('name').type('varchar').length(100)\n
        table.build()\n
        :param table_name: 表名
        :param table_comment: 表的备注
        :return:
        """
        connect = self._pool_.connection()
        cursor = connect.cursor()
        return PostgresqlCreateTable(connect, cursor, table_name, table_comment=table_comment)

    def create_database(self, database_name, owner=None, encoding="UTF8", tablespace='pg_default',
                        local_collate='zh_CN.UTF-8', local_ctype='zh_CN.UTF-8', connection_limit=-1):
        """
        创建数据库
        :param local_collate: 字符排序规则
        :param connection_limit: 最大并发连接数，-1 表示没有连接限制
        :param tablespace: 默认表空间
        :param encoding: 字符编码
        :param owner: 数据库的所有者
        :param local_ctype: 字符分类
        :param database_name: 数据库名
        :return:
        """
        if owner is not None:
            sql = f"CREATE DATABASE {database_name} WITH OWNER = {owner} ENCODING = {encoding} TABLESPACE = {tablespace} LC_COLLATE = '{local_collate}' LC_CTYPE = '{local_ctype}' CONNECTION LIMIT = {connection_limit};"
        else:
            sql = f"CREATE DATABASE {database_name} WITH ENCODING = {encoding} TABLESPACE = {tablespace} LC_COLLATE = '{local_collate}' LC_CTYPE = '{local_ctype}' CONNECTION LIMIT = {connection_limit};"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def drop_table(self, table_name):
        """
        删除表
        :param table_name: 表名
        :return:
        """
        sql = f"DROP TABLE IF EXISTS {table_name};"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        connect.commit()
        cursor.close()
        connect.close()
        cursor.execute(sql)

    def show_table(self):
        """
        显示数据库中所有表名
        :return:
        """
        sql = f"SELECT tablename FROM pg_tables;"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def show_database(self):
        """
        显示所有数据库
        :return:
        """
        sql = f"SELECT datname FROM pg_database;"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def drop_database(self, database_name):
        """
        删除数据库
        :param database_name: 数据库名
        :return:
        """
        sql = f"DROP DATABASE IF EXISTS {database_name}"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def alter_table_name(self, table_name, new_table_name):
        """
        更改表名
        :param table_name: 现在的表名
        :param new_table_name: 新的表名
        :return:
        """
        sql = f"ALTER TABLE {table_name} RENAME TO {new_table_name}"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def drop_column(self, table_name, column):
        """
        删除表中的某个字段
        :param table_name: 表名
        :param column: 字段名
        :return:
        """
        sql = f"ALTER TABLE {table_name} DROP COLUMN {column};"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def alter_column_type(self, table_name: str, column_name: str, column_type: str, length: int,
                          is_not_null: bool = True, is_primary_key: str = False, is_auto_increment: str = False,
                          using: str = None):
        """
        更改表中某字段的类型
        :param table_name: 表名
        :param column_name: 字段名
        :param column_type: 字段类型
        :param length: 长度
        :param is_not_null: 是否为空
        :param is_primary_key: 是否为关键字
        :param is_auto_increment: 是否自增
        :return:
        """
        constraint = ""
        if is_not_null:
            constraint += " NOT NULL"
        if is_primary_key:
            constraint += " PRIMARY KEY"
        if is_auto_increment:
            constraint += " SERIAL"
        if using is not None:
            constraint += f" USING {using}"
        sql = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {column_type}({length})"
        sql += constraint
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def alter_column_name(self, table_name: str, column_name: str, new_column_name: str, column_type: str, length: int):
        """
        更改表中某字段的名字
        :param new_column_name: 新的字段名
        :param table_name: 表名
        :param column_name: 字段名
        :param column_type: 字段类型
        :param length: 长度
        :return:
        """
        sql = f"ALTER TABLE {table_name} RENAME COLUMN {column_name} TO {new_column_name} {column_type}({length});"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def add_column(self, table_name: str, column_name: str, column_type: str = "varchar", length: int = 255,
                   is_not_null: bool = True, is_primary_key: str = False, is_auto_increment: str = False):
        """
        向表中新增字段
        :param table_name: 表名
        :param column_name: 字段名
        :param column_type: 字段类型
        :param length: 长度
        :param is_not_null: 是否为空
        :param is_primary_key: 是否为关键字
        :param is_auto_increment: 是否自增
        :return:
        """
        constraint = ""
        if is_not_null:
            constraint += " NOT NULL"
        if is_primary_key:
            constraint += " PRIMARY KEY"
        if is_auto_increment:
            constraint += " SERIAL"
        sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}({length})"
        sql += constraint
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def create_index(self, table_name: str, column_name: str, index_name: str):
        """
        创建索引
        :param table_name: 表名
        :param column_name: 列名
        :param index_name: 索引名
        :return:
        """
        sql = f"CREATE INDEX {index_name} ON {table_name} ({column_name});"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def create_unique_index(self, table_name: str, column_name: str, index_name: str):
        """
        创建唯一索引
        :param table_name: 表名
        :param column_name: 列名
        :param index_name: 索引名
        :return:
        """
        sql = f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({column_name});"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def drop_index(self, table_name: str, index_name: str):
        """
        删除索引
        :param table_name: 表名
        :param index_name: 索引名
        :return:
        """
        sql = f"DROP INDEX {index_name};"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def show_model(self):
        """
        显示模式列表
        :return:
        """
        connect = self._pool_.connection()
        cursor = connect.cursor()
        sql = f"SELECT SCHEMA_NAME FROM information_schema.schemata;;"
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def show_table_by_database_name(self, name: str):
        """
        显示指定数据库的表
        :param name: 数据库名
        :return:
        """
        connect = self._pool_.connection()
        cursor = connect.cursor()
        sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{name}';"
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def show_columns(self, database, table: str):
        columns = ["COLUMN_NAME", "DATA_TYPE"]
        dt = self.select("INFORMATION_SCHEMA.COLUMNS", columns).multi_condition_query(
            [{"name": "TABLE_SCHEMA",
              "value": database,
              "logical_condition": "and",
              "judgement_condition": "="
              },
             {"name": "TABLE_NAME ",
              "value": table,
              "logical_condition": "and",
              "judgement_condition": "="
              }
             ]).run()
        return dt
