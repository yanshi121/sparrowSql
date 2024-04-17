import pyodbc


class SqlServer:
    def __init__(self, host, port, user, password, database=None):
        self._host_ = host
        self._port_ = port
        self._user_ = user
        self._passwd_ = password
        self._db_ = database
        self._connect_ = ""
        self._cursor_ = ""
        self._get_connect_()

    def _get_connect_(self):
        """
        获取数据库连接
        :return:
        """
        if self._db_ is not None:
            self.conn_str = 'DRIVER={SQL Server};SERVER=' + self._host_ + ',' + self._port_ + ';DATABASE=' + self._db_ + ';UID=' + self._user_ + ';PWD=' + self._passwd_
        else:
            self.conn_str = 'DRIVER={SQL Server};SERVER=' + self._host_ + ',' + self._port_ + ';UID=' + self._user_ + ';PWD=' + self._passwd_
        self._connect_ = pyodbc.connect(self.conn_str)
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
                set_str += f"{column}=?,"
        if len(conditions) != 0:
            where_str += "where "
            for condition_key, condition_value in conditions.items():
                if condition_value != '':
                    params += (condition_value,)
                    where_str += f"{condition_key}=? and "
        sql = f"update {table} set {set_str[:-1]} {where_str[:-5]};"
        self._cursor_.execute(sql, params)

    class _CreateTable:
        """
        创建表的类
        """

        def __init__(self, connect, cursor, table_name: str):
            self._connect_ = connect
            self._cursor_ = cursor
            self._table_name_ = table_name
            self._columns_ = []
            self._primary_key_column_ = None
            self._sql_ = f"CREATE TABLE {self._table_name_} (\n"

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
                "auto_increment": None,
                "unique": False
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

            def auto_increment(self, auto_increment: tuple):
                """
                添加字段是否自增
                :return:
                """
                if self._column_["type"] != "int":
                    raise ValueError("Auto-increment can only be used with integer columns.")
                self._column_["auto_increment"] = auto_increment
                return self

            def unique(self, column_comment=True):
                """
                添加字段的注释
                :param column_comment:
                :return:
                """
                self._column_["unique"] = column_comment
                return self

        def build(self):
            """
            构建SQL并执行
            :return:
            """
            column_definitions = []
            for column in self._columns_:
                if column['auto_increment'] is not None:
                    definition = f"{column['name']} {column['type']}"
                    definition += f" IDENTITY{column['auto_increment']}"
                else:
                    definition = f"{column['name']} {column['type']}({column['length']})"
                if column['null']:
                    definition += " NOT NULL"
                if column['primary_key']:
                    definition += " PRIMARY KEY"
                if column['unique']:
                    definition += f" UNIQUE"
                column_definitions.append(definition)
            self._sql_ += ",\n".join(column_definitions)
            self._sql_ += "\n)"
            self._cursor_.execute(self._sql_)
            self._connect_.commit()

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
        return self._CreateTable(self._connect_, self._cursor_, table_name)

    def show_database(self):
        """
        显示所有数据库
        :return:
        """
        sql = f"SELECT name FROM sys.databases;"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

    def show_table(self):
        """
        显示数据库中所有表名
        :return:
        """
        sql = f"SELECT name FROM sys.tables;"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

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
                condition_str += f"{column}=? and "
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

    def select_page(self, table: str, sort_column: str, conditions: dict = None, columns: list = None, page_size: int = 20,
                    page_index: int = 0, sort_method: str = "asc"):
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
                condition_str += f"{column}=? and "
            condition_str = condition_str[:-5]
            sql = f"select {column_str} from {table} where {condition_str}"
        else:
            sql = f"select {column_str} from {table}"
        if sort_method == "asc":
            sql += f" order by {sort_column} asc"
        elif sort_method == "desc":
            sql += f" order by {sort_column} desc"
        page_index = page_index * page_size
        params += (page_index, page_size)
        sql += f" offset ? rows fetch next ? rows only;"
        print(sql)
        self._cursor_.execute(sql, params)
        row = self._cursor_.fetchall()
        return row


if __name__ == '__main__':
    app = SqlServer("192.168.233.131", "1433", "sa", "aDYLL121380O!")
    print(app.select_page("test", 'id'))
