import pymysql
from colorama import Fore, init


class MySQL(object):

    def __init__(self, host: str, port: int, user: str, passwd: str, db: str):
        self._host_ = host
        self._port_ = port
        self._user_ = user
        self._passwd_ = passwd
        self._db_ = db
        self._connect_: pymysql.connect = ""
        self._cursor_: pymysql.connect.cursor = ""
        self._get_connect_()
        init(autoreset=True)

    def _get_connect_(self):
        self._connect_ = pymysql.connect(host=self._host_, user=self._user_, password=self._passwd_,
                                         port=self._port_, database=self._db_)
        self._cursor_ = self._connect_.cursor()
        if not self._cursor_:
            raise Exception(f"{self._host_}--数据库连接失败")

    def connect_information(self):
        return {
            "host": self._host_,
            "port": self._port_,
            "user": self._user_,
            "passwd": self._passwd_,
            "db": self._db_
        }

    def close(self):
        self._cursor_.close()
        self._connect_.close()

    def commit(self):
        self._connect_.commit()

    def commit_close(self):
        self.commit()
        self.close()

    def user_defined_sql(self, sql: str, params: tuple = None):
        if params is None:
            self._cursor_.execute(sql)
        else:
            self._cursor_.execute(sql, params)
        row = self._cursor_.fetchall()
        return row

    def insert(self, table: str, columns: list, values: list):
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
                        params += (value_s, )
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
        if type(columns_values) != dict:
            raise Exception(f"columns_values {columns_values} type is not dict")
        if type(conditions) != dict:
            raise Exception(f"conditions {conditions} type is not dict")
        set_str = ""
        where_str = ""
        params = ()
        for column, value in columns_values.items():
            if value != "":
                params += (value, )
                set_str += f"{column}=%s,"
        if len(conditions) != 0:
            where_str += "where "
            for condition_key, condition_value in conditions.items():
                if condition_value != '':
                    params += (condition_value, )
                    where_str += f"{condition_key}=%s and "
        sql = f"update {table} set {set_str[:-1]} {where_str[:-5]};"
        self._cursor_.execute(sql, params)

    def delete(self, table: str, conditions: dict):
        if type(conditions) != dict:
            raise Exception(f"conditions {conditions} type is not dict")
        delete_str = ""
        params = ()
        for column, value in conditions.items():
            params += (value, )
            delete_str += f"{column}=%s and "
        sql = f"delete from {table} where {delete_str[:-5]};"
        self._cursor_.execute(sql, params)

    def select(self, table: str, conditions: dict = None, columns: list = None, sort_column: str = None,
               sort_method: str = "asc"):
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
                params += (value, )
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
                params += (value, )
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
        def __init__(self, connect, cursor, table_name: str, table_comment: str = None):
            self._connect_ = connect
            self._table_comment_ = table_comment
            self._cursor_ = cursor
            self._table_name_ = table_name
            self._columns_ = []
            self._primary_key_column_ = None
            self._sql_ = f"CREATE TABLE IF NOT EXISTS `{self._table_name_}` (\n"

        def column(self, column_name: str):
            for column in self._columns_:
                if column_name in column.values():
                    raise Exception(f"Column {column_name} already exists")
            column = {
                "name": column_name,
                "type": "varchar",
                "length": 255,
                "null": True,
                "primary_key": False,
                "auto_increment": False
            }
            self._columns_.append(column)
            return self._ColumnBuilder(self, column)

        class _ColumnBuilder:
            def __init__(self, parent, column):
                self._parent_ = parent
                self._column_ = column

            def type(self, column_type: str):
                self._column_["type"] = column_type
                return self

            def length(self, length: int):
                self._column_["length"] = length
                return self

            def is_null(self, is_nullable: bool):
                self._column_["null"] = is_nullable
                return self

            def primary_key(self):
                if self._parent_._primary_key_column_ is not None:
                    raise ValueError("Only one primary key column can be set.")
                self._column_["primary_key"] = True
                self._parent_._primary_key_column_ = self._column_
                return self

            def auto_increment(self):
                if self._column_["type"] != "int":
                    raise ValueError("Auto-increment can only be used with integer columns.")
                self._column_["auto_increment"] = True
                return self

            def comment(self, column_comment):
                self._column_["comment"] = column_comment
                return self

        def build(self):
            column_definitions = []
            for column in self._columns_:
                definition = f"`{column['name']}` {column['type']}({column['length']})"
                if not column['null']:
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
        return self._CreateTable(self._connect_, self._cursor_, table_name, table_comment=table_comment)

    def drop_table(self, table_name):
        sql = f"DROP TABLE IF EXISTS {table_name};"
        self._cursor_.execute(sql)

    def show_table(self):
        sql = f"SHOW TABLES;"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

    def show_database(self):
        sql = f"SHOW DATABASES;"
        self._cursor_.execute(sql)
        row = self._cursor_.fetchall()
        return row

    def drop_database(self, database_name):
        sql = f"DROP DATABASE IF EXISTS {database_name}"
        self._cursor_.execute(sql)

    def alter_table(self, table_name, new_table_name):
        sql = f"ALTER TABLE {table_name} RENAME TO {new_table_name}"
        self._cursor_.execute(sql)


if __name__ == '__main__':
    run = MySQL("localhost", 3306, "root", "root123", "recruit")
    print(run.select_page("work_boos"))

