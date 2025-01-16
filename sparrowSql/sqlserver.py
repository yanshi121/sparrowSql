import pyodbc
from dbutils.pooled_db import PooledDB

from sparrowSql.tools import SQLServerCreateTable, ConditionsBuilder, SQLServerSelectConditionsBuilder


class SqlServer:
    def __init__(self, host, port, user, passwd, db=None, max_connections: int = 50):
        """
        Sparrow for SqlServer
        :param host: 数据地址
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
            creator=pyodbc,
            maxconnections=self._max_connections_,
            **{
                'DRIVER': '{Sql Server}',
                'SERVER': self._host_,
                'PORT': self._port_,
                'DATABASE': self._db_,
                'UID': self._user_,
                'PWD': self._passwd_,
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
        输入参数: user_defined_sql('select name from user where id = ?', (1))
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
                    value_list.append("(" + ", ".join(["?" for i in value]) + ")")
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
                values = "(" + ", ".join(["?" for i in values]) + ")"
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

    def create_table(self, table_name):
        """
        创建表\n
        table = create_table("test")\n
        table.column('id').type('int').length(10)\n
        table.column('name').type('varchar').length(100)\n
        table.build()\n
        :param table_name: 表名
        :return:
        """
        connect = self._pool_.connection()
        cursor = connect.cursor()
        return SQLServerCreateTable(connect, cursor, table_name)

    def show_database(self):
        """
        显示所有数据库
        :return:
        """
        sql = f"SELECT name FROM sys.databases;"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def show_table(self):
        """
        显示数据库中所有表名
        :return:
        """
        sql = f"SELECT name FROM sys.tables;"
        connect = self._pool_.connection()
        cursor = connect.cursor()
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

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
        return SQLServerSelectConditionsBuilder(head_sql, cursor, connect, columns)

    def show_table_by_database_name(self, name: str):
        return self.show_table()

    def show_columns(self, database, table: str):
        connect = self._pool_.connection()
        cursor = connect.cursor()
        sql = f"SELECT c.name, t.name FROM  {database}.sys.columns c INNER JOIN {database}.sys.types t ON c.user_type_id = t.user_type_id INNER JOIN {database}.sys.tables tbl ON c.object_id = tbl.object_id WHERE tbl.name = '{table}';"
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row
