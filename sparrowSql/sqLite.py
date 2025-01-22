import sqlite3
from sparrowSql.tools import ConditionsBuilder, SelectConditionsBuilder, SqliteCreateTable


class SqLite:
    def __init__(self, db_file: str):
        """
        连接sqLite
        :param db_file: sqLite文件路径
        """
        self._connect_ = sqlite3.connect(db_file)
        self._cursor_ = self._connect_.cursor()

    def close(self):
        self._cursor_.close()
        self._connect_.close()

    def commit(self):
        self._connect_.commit()

    def commit_close(self):
        self.commit()
        self.close()

    def user_defined_sql(self, sql: str, params: tuple = None):
        """
        运行自定义SQL\n
        在不输入参数: user_defined_sql('select name from user where id = 1')\n
        输入参数: user_defined_sql('select name from user where id = ?', (1))
        :param sql: SQL
        :param params: 参数，输入参数为参数化查询
        :return:
        """
        if params is None:
            self._cursor_.execute(sql)
        else:
            self._cursor_.execute(sql, params)
        row = self._cursor_.fetchall()
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
            self._cursor_.execute(sql, params)
            return sql
        else:
            params = ()
            if len(values) == len(columns):
                for value_s in values:
                    params += (value_s,)
                values = "(" + ", ".join(["?" for i in values]) + ")"
                sql = f"insert into {table} {column} values {values};"
                self._cursor_.execute(sql, params)
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
        return ConditionsBuilder(head_sql, self._cursor_)

    def delete(self, table: str):
        """
        删除数据
        :param table: 表名
        :return:
        """
        head_sql = f"DELETE FROM {table}"
        return ConditionsBuilder(head_sql, self._cursor_)

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
            columns_str = "'" + "', '".join(columns) + "'"
        head_sql = f"SELECT {columns_str} FROM {table}"
        return SelectConditionsBuilder(head_sql, self._cursor_)

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
        return SqliteCreateTable(self._connect_, self._cursor_, table_name, table_comment=table_comment)

    def drop_table(self, table_name):
        """
        删除表
        :param table_name: 表名
        :return:
        """
        sql = f"DROP TABLE IF EXISTS {table_name};"
        self._cursor_.execute(sql)

    def show_table(self):
        """
        显示数据库中所有表名
        :return:
        """
        sql = f"SELECT name FROM sqlite_master WHERE type='table';"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

    def alter_table_name(self, table_name, new_table_name):
        """
        更改表名
        :param table_name: 现在的表名
        :param new_table_name: 新的表名
        :return:
        """
        sql = f"ALTER TABLE {table_name} RENAME TO {new_table_name}"
        self._cursor_.execute(sql)

    def drop_column(self, table_name, column):
        """
        删除表中的某个字段
        :param table_name: 表名
        :param column: 字段名
        :return:
        """
        sql = f"ALTER TABLE {table_name} DROP {column};"
        self._cursor_.execute(sql)

    def alter_column_type(self, table_name: str, column_name: str, column_type: str, length: int,
                          is_not_null: bool = True,
                          is_primary_key: str = False, is_auto_increment: str = False):
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
            constraint += " AUTOINCREMENT"
        sql = f"ALTER TABLE {table_name} MODIFY {column_name} {column_type}({length})"
        sql += constraint
        self._cursor_.execute(sql)

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
        sql = f"ALTER TABLE {table_name} CHANGE {column_name} {new_column_name} {column_type}({length})"
        self._cursor_.execute(sql)

    def add_column(self, table_name: str, column_name: str, column_type: str = "varchar", length: int = 255,
                   is_not_null: bool = True, is_primary_key: str = False, is_auto_increment: str = False,
                   is_first: bool = False):
        """
        向表中新增字段
        :param is_first: True将新加的属性设置为该表的第一个字段,False将新加的字段置于该表其余字段之后
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
            constraint += " AUTOINCREMENT"
        sql = f"ALTER TABLE {table_name} ADD {column_name} {column_type}({length})"
        sql += constraint
        if is_first:
            sql += " FIRST"
        self._cursor_.execute(sql)

    def create_index(self, table_name: str, column_name: str, index_name: str):
        """
        创建索引
        :param table_name: 表名
        :param column_name: 列名
        :param index_name: 索引名
        :return:
        """
        sql = f"CREATE INDEX {index_name} ON {table_name} ({column_name});"
        self._cursor_.execute(sql)

    def create_unique_index(self, table_name: str, column_name: str, index_name: str):
        """
        创建唯一索引
        :param table_name: 表名
        :param column_name: 列名
        :param index_name: 索引名
        :return:
        """
        sql = f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({column_name});"
        self._cursor_.execute(sql)

    def drop_index(self, table_name: str, index_name: str):
        """
        删除索引
        :param table_name: 表名
        :param index_name: 索引名
        :return:
        """
        sql = f"DROP INDEX {index_name}_on_{table_name};"
        self._cursor_.execute(sql)

    def start_transaction(self):
        """
        开始事务
        :return:
        """
        sql = "BEGIN TRANSACTION;"
        self._cursor_.execute(sql)
        print("事务开启...")

    def rollback_transaction(self):
        """
        回滚事务
        :return:
        """
        sql = "ROLLBACK;"
        self._cursor_.execute(sql)
        print("事务回滚...")

    def commit_transaction(self):
        """
        提交事务
        :return:
        """
        sql = "COMMIT;"
        self._cursor_.execute(sql)
        print("事务提交...")
