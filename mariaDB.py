import pymysql
from SQL import SQl


class MariaDB(SQl):

    def __init__(self, host: str, port: int, user: str, passwd: str, db: str = None):
        self._host_ = host
        self._port_ = port
        self._user_ = user
        self._passwd_ = passwd
        self._db_ = db
        self._connect_: pymysql.connect = ""
        self._cursor_: pymysql.connect.cursor = ""
        self._get_connect_()

    def _get_connect_(self):
        """
        获取数据库连接
        :return:
        """
        if self._db_ is not None:
            self._connect_ = pymysql.connect(host=self._host_, user=self._user_, password=self._passwd_,
                                             port=self._port_)
        else:
            self._connect_ = pymysql.connect(host=self._host_, user=self._user_, password=self._passwd_,
                                             port=self._port_, database=self._db_)
        self._cursor_ = self._connect_.cursor()
        if not self._cursor_:
            raise Exception(f"{self._host_}--数据库连接失败")

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

    def close(self):
        """
        关闭游标和连接
        :return:
        """
        self._cursor_.close()
        self._connect_.close()

    def commit(self):
        """
        提交
        :return:
        """
        self._connect_.commit()

    def commit_close(self):
        """
        提交并关闭
        :return:
        """
        self.commit()
        self.close()

    def user_defined_sql(self, sql: str, params: tuple = None):
        """
        运行自定义SQL\n
        在不输入参数: user_defined_sql('select name from user where id = 1')\n
        输入参数: user_defined_sql('select name from user where id = %s', (1))
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
                    value_list.append("(" + ", ".join(["%s" for i in value]) + ")")
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
                values = "(" + ", ".join(["%s" for i in values]) + ")"
                sql = f"insert into {table} {column} values {values};"
                self._cursor_.execute(sql, params)
                return sql
            else:
                raise Exception(f"{columns}->{len(columns)} != {values}->{len(values)}")

    def update(self, table: str, columns_values: dict, conditions: dict):
        """
        更新数据
        :param table: 表名
        :param columns_values: 修改的数据
        :param conditions: 条件参数
        :return:
        """
        if type(columns_values) != dict:
            raise Exception(f"columns_values {columns_values} type is not dict")
        if type(conditions) != dict:
            raise Exception(f"conditions {conditions} type is not dict")
        set_str = ""
        where_str = ""
        params = ()
        for column, value in columns_values.items():
            if value != "":
                params += (value,)
                set_str += f"{column}=%s,"
        if len(conditions) != 0:
            where_str += "where "
            for condition_key, condition_value in conditions.items():
                if condition_value != '':
                    params += (condition_value,)
                    where_str += f"{condition_key}=%s and "
        sql = f"update {table} set {set_str[:-1]} {where_str[:-5]};"
        self._cursor_.execute(sql, params)

    def delete(self, table: str, conditions: dict):
        """
        删除数据
        :param table: 表名
        :param conditions: 删除参数
        :return:
        """
        if type(conditions) != dict:
            raise Exception(f"conditions {conditions} type is not dict")
        delete_str = ""
        params = ()
        for column, value in conditions.items():
            params += (value,)
            delete_str += f"{column}=%s and "
        sql = f"delete from {table} where {delete_str[:-5]};"
        self._cursor_.execute(sql, params)

    def select(self, table: str, conditions: dict = None, columns: list = None, sort_column: str = None,
               sort_method: str = "asc"):
        """
        查询数据
        :param table: 表名
        :param conditions: 查询条件，不输入默认为全部
        :param columns: 字段名，不输入默认为所有字段
        :param sort_column: 需要排序的字段
        :param sort_method: 排序方法，默认为asc升序，desc为降序
        :return:
        """
        params = ()
        if columns is not None:
            if type(conditions) != dict:
                raise Exception(f"conditions {conditions} type is not dict")
        if columns is None:
            column_str = "*"
        else:
            if type(columns) != list:
                raise Exception(f"columns {columns} type is not list")
            column_str = ",".join(columns)
        condition_str = ""
        if conditions is not None:
            for column, value in conditions.items():
                params += (value,)
                condition_str += f"{column}=%s and "
            condition_str = condition_str[:-5]
            sql = f"select {column_str} from {table} where {condition_str}"
        else:
            sql = f"select {column_str} from {table}"
        if sort_column is not None:
            if sort_method == "asc":
                sql += f" order by {sort_column} asc"
            elif sort_method == "desc":
                sql += f" order by {sort_column} desc"
        sql += ";"
        self._cursor_.execute(sql, params)
        row = self._cursor_.fetchall()
        return row

    def select_page(self, table: str, conditions: dict = None, columns: list = None, page_size: int = 20,
                    page_index: int = 0,
                    sort_column: str = None, sort_method: str = ""):
        """
        分页查询
        :param table: 表名
        :param conditions: 查询参数。默认为全部
        :param columns: 字段名，默认为全部
        :param page_size: 分页大小，默认20
        :param page_index: 当前页码，默认为0
        :param sort_column: 排序字段
        :param sort_method: 排序方法，默认为asc升序，desc为降序
        :return:
        """
        params = ()
        if conditions is not None:
            if type(conditions) != dict:
                raise Exception(f"conditions {conditions} type is not dict")
        if columns is None:
            column_str = "*"
        else:
            if type(columns) != list:
                raise Exception(f"columns {columns} type is not list")
            column_str = ",".join(columns)
        condition_str = ""
        if conditions is not None:
            for column, value in conditions.items():
                params += (value,)
                condition_str += f"{column}=%s and "
            condition_str = condition_str[:-5]
            sql = f"select {column_str} from {table} where {condition_str}"
        else:
            sql = f"select {column_str} from {table}"
        if sort_column is not None:
            if sort_method == "asc":
                sql += f" order by {sort_column} asc"
            elif sort_method == "desc":
                sql += f" order by {sort_column} desc"
        page_index = page_index * page_size
        params += (page_size, page_index)
        sql += f" limit %s offset %s"
        self._cursor_.execute(sql, params)
        row = self._cursor_.fetchall()
        return row

    class _CreateTable:
        """
        创建表的类
        """

        def __init__(self, connect, cursor, table_name: str, table_comment: str = None):
            self._connect_ = connect
            self._table_comment_ = table_comment
            self._cursor_ = cursor
            self._table_name_ = table_name
            self._columns_ = []
            self._primary_key_column_ = None
            self._sql_ = f"CREATE TABLE IF NOT EXISTS `{self._table_name_}` (\n"

        def column(self, column_name: str):
            """
            初始化字段
            :param column_name: 字段名
            :return:
            """
            for column in self._columns_:
                if column_name in column.values():
                    raise Exception(f"Column {column_name} already exists")
            column = {
                "name": column_name,
                "type": "varchar",
                "length": 255,
                "null": False,
                "primary_key": False,
                "auto_increment": False
            }
            self._columns_.append(column)
            return self._ColumnBuilder(self, column)

        class _ColumnBuilder:
            """
            为字段添加参数
            """

            def __init__(self, parent, column):
                self._parent_ = parent
                self._column_ = column

            def type(self, column_type: str):
                """
                添加字段类型
                :param column_type: 输入数据库支持的类型
                :return:
                """
                self._column_["type"] = column_type
                return self

            def length(self, length: int):
                """
                添加字段长度
                :param length:
                :return:
                """
                self._column_["length"] = length
                return self

            def is_not_null(self, is_nullable: bool = True):
                """
                添加点断是否为不能空
                :param is_nullable: 输入False为可空，默认为True
                :return:
                """
                self._column_["null"] = is_nullable
                return self

            def primary_key(self):
                """
                添加字段是否是关键字
                :return:
                """
                if self._parent_._primary_key_column_ is not None:
                    raise ValueError("Only one primary key column can be set.")
                self._column_["primary_key"] = True
                self._parent_._primary_key_column_ = self._column_
                return self

            def auto_increment(self):
                """
                添加字段是否自增
                :return:
                """
                if self._column_["type"] != "int":
                    raise ValueError("Auto-increment can only be used with integer columns.")
                self._column_["auto_increment"] = True
                return self

            def comment(self, column_comment):
                """
                添加字段的注释
                :param column_comment:
                :return:
                """
                self._column_["comment"] = column_comment
                return self

        def build(self):
            """
            构建SQL并执行
            :return:
            """
            column_definitions = []
            for column in self._columns_:
                definition = f"`{column['name']}` {column['type']}({column['length']})"
                if column['null']:
                    definition += " NOT NULL"
                if column['primary_key']:
                    definition += " PRIMARY KEY"
                if column['auto_increment']:
                    definition += " AUTO_INCREMENT"
                if 'comment' in column and column['comment']:
                    definition += f" COMMENT '{column['comment']}'"
                column_definitions.append(definition)
            self._sql_ += ",\n".join(column_definitions)
            self._sql_ += "\n)"
            if self._table_comment_ is not None:
                self._sql_ += f"COMMENT='{self._table_comment_}'"
            self._cursor_.execute(self._sql_)
            self._connect_.commit()

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
        return self._CreateTable(self._connect_, self._cursor_, table_name, table_comment=table_comment)

    def create_database(self, database_name, character="utf8mb4", collate="utf8mb4_general_ci"):
        """
        创建数据库
        :param database_name: 数据库名
        :param character: 字符集
        :param collate: 校对规则
        :return:
        """
        sql = f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET {character} COLLATE {collate}"
        self._cursor_.execute(sql)

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
        sql = f"SHOW TABLES;"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

    def show_database(self):
        """
        显示所有数据库
        :return:
        """
        sql = f"SHOW DATABASES;"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

    def drop_database(self, database_name):
        """
        删除数据库
        :param database_name: 数据库名
        :return:
        """
        sql = f"DROP DATABASE IF EXISTS {database_name}"
        self._cursor_.execute(sql)

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

    def add_column(self, table_name: str, column_name: str, column_type: str, length: int, is_not_null: bool = True,
                   is_primary_key: str = False, is_auto_increment: str = False, is_first: bool = False):
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

