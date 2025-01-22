class SQLConditionsBuilderBass:
    def __init__(self, head_sql, cursor, connect):
        self._cursor = cursor
        self._connect = connect
        self._head_sql = head_sql
        self._and_where_clauses = []
        self._or_where_clauses = []

    def and_between(self, column: str, start: str, end: str):
        """
        构建介于两者之间的单表并列查询条件
        :param column: 字段名
        :param start: 开始位置
        :param end: 结束位置
        :return:
        """
        self._and_where_clauses.append(f"`{column}` BETWEEN '{start}' AND '{end}'")
        return self

    def or_between(self, column: str, start: str, end: str):
        """
        构建介于两者之间的单表或查询条件
        :param column: 字段名
        :param start: 开始位置
        :param end: 结束位置
        :return:
        """
        self._or_where_clauses.append(f"`{column}` BETWEEN '{start}' AND '{end}'")
        return self

    def and_equal(self, column: str, value: str):
        """
        构建等于的单表并列查询条件
        :param column: 字段名
        :param value: 开始位置
        :return:
        """
        self._and_where_clauses.append(f"`{column}` = '{value}'")
        return self

    def or_equal(self, column: str, value: str):
        """
        构建等于的单表或查询条件
        :param column: 字段名
        :param value: 开始位置
        :return:
        """
        self._or_where_clauses.append(f"`{column}` = '{value}'")
        return self

    def and_unequal(self, column: str, value: str):
        """
        构建不等于的单表并列查询条件
        :param column: 字段名
        :param value: 开始位置
        :return:
        """
        self._and_where_clauses.append(f"`{column}` != '{value}'")
        return self

    def or_unequal(self, column: str, value: str):
        """
        构建不等于的单表或查询条件
        :param column: 字段名
        :param value: 开始位置
        :return:
        """
        self._or_where_clauses.append(f"`{column}` != '{value}'")
        return self

    def and_equal_greater(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` >= '{value}'")
        return self

    def or_equal_greater(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` >= '{value}'")

    def and_equal_less(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` <= '{value}'")
        return self

    def or_unequal_less(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` <= '{value}'")
        return self

    def and_greater(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` > '{value}'")
        return self

    def or_greater(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` > '{value}'")
        return self

    def and_less(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` < '{value}'")
        return self

    def or_less(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` < '{value}'")
        return self

    def and_like_start(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` like '%{value}'")
        return self

    def or_like_start(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` like '%{value}'")
        return self

    def and_like_end(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` like '{value}%'")
        return self

    def or_like_end(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` like '{value}%'")
        return self

    def and_like(self, column: str, value: str):
        self._and_where_clauses.append(f"`{column}` like '%{value}%'")
        return self

    def or_like(self, column: str, value: str):
        self._or_where_clauses.append(f"`{column}` like '%{value}%'")
        return self

    def and_in(self, column: str, value: list):
        if type(value) is not list:
            raise TypeError("value must be list")
        dt = "', '".join(value)
        sql = "('" + f"{dt}" + "')"
        self._and_where_clauses.append(f"`{column}` IN {sql}")
        return self

    def or_in(self, column: str, value: list):
        if type(value) is not list:
            raise TypeError("value must be list")
        dt = "', '".join(value)
        sql = "('" + f"{dt}" + "')"
        self._or_where_clauses.append(f"`{column}` IN {sql}")
        return self

    def and_not_in(self, column: str, value: list):
        if type(value) is not list:
            raise TypeError("value must be list")
        dt = "', '".join(value)
        sql = "('" + f"{dt}" + "')"
        self._and_where_clauses.append(f"`{column}` NOT IN {sql}")
        return self

    def or_not_in(self, column: str, value: list):
        if type(value) is not list:
            raise TypeError("value must be list")
        dt = "', '".join(value)
        sql = "('" + f"{dt}" + "')"
        self._or_where_clauses.append(f"`{column}` NOT IN {sql}")
        return self

    def and_between_cross_table(self, column: str, start: str, end: str):
        self._and_where_clauses.append(f"{column} BETWEEN {start} AND {end}")
        return self

    def or_between_cross_table(self, column: str, start: str, end: str):
        self._or_where_clauses.append(f"{column} BETWEEN {start} AND {end}")
        return self

    def and_equal_cross_table(self, column: str, value: str):
        self._and_where_clauses.append(f"{column} = {value}")
        return self

    def or_equal_cross_table(self, column: str, value: str):
        self._or_where_clauses.append(f"{column} = {value}")
        return self

    def and_unequal_cross_table(self, column: str, value: str):
        self._and_where_clauses.append(f"{column} != {value}")
        return self

    def or_unequal_cross_table(self, column: str, value: str):
        self._or_where_clauses.append(f"{column} != {value}")
        return self

    def and_equal_greater_cross_table(self, column: str, value: str):
        self._and_where_clauses.append(f"{column} >= {value}")
        return self

    def or_equal_greater_cross_table(self, column: str, value: str):
        self._or_where_clauses.append(f"{column} >= {value}")

    def and_equal_less_cross_table(self, column: str, value: str):
        self._and_where_clauses.append(f"{column} <= {value}")
        return self

    def or_unequal_less_cross_table(self, column: str, value: str):
        self._or_where_clauses.append(f"{column} <= {value}")
        return self

    def and_greater_cross_table(self, column: str, value: str):
        self._and_where_clauses.append(f"{column} > {value}")
        return self

    def or_greater_cross_table(self, column: str, value: str):
        self._or_where_clauses.append(f"{column} > {value}")
        return self

    def and_less_cross_table(self, column: str, value: str):
        self._and_where_clauses.append(f"{column} < {value}")
        return self

    def or_less_cross_table(self, column: str, value: str):
        self._or_where_clauses.append(f"{column} < {value}")
        return self

    def and_in_cross_table(self, column: str, value: str):
        if type(value) is not str:
            raise TypeError("value must be str")
        self._and_where_clauses.append(f"{column} IN {value}")
        return self

    def or_in_cross_table(self, column: str, value: str):
        if type(value) is not str:
            raise TypeError("value must be str")
        self._or_where_clauses.append(f"{column} IN {value}")
        return self

    def and_not_in_cross_table(self, column: str, value: str):
        if type(value) is not str:
            raise TypeError("value must be str")
        self._and_where_clauses.append(f"{column} NOT IN {value}")
        return self

    def or_not_in_cross_table(self, column: str, value: str):
        if type(value) is not str:
            raise TypeError("value must be str")
        self._or_where_clauses.append(f"{column} NOT IN {value}")
        return self

    def and_is_null(self, column: str):
        self._and_where_clauses.append(f"{column} IS NULL")
        return self

    def or_is_null(self, column: str):
        self._or_where_clauses.append(f"{column} IS NULL")
        return self

    def and_is_not_null(self, column: str):
        self._and_where_clauses.append(f"{column} IS NOT NULL")
        return self

    def or_is_not_null(self, column: str):
        self._or_where_clauses.append(f"{column} IS NOT NULL")
        return self

    def multi_condition_query(self, conditions: list):
        """
        :param conditions: 多条件参数 [{"name": ""(str), "value": ""(str,list),
        "logical_condition": ""(str), "judgement_condition": ""(str)}]
        其中，name是字段名，value字段值，logical_condition是条件间逻辑包含（and, or），
        judgement_condition是判断模式包含（>,<,=,>=,<=,in,not in等）
        :return:
        """
        for condition in conditions:
            column_name = condition['name']
            column_value = condition['value']
            column_logical_condition = condition['logical_condition']
            column_judgement_condition = condition['judgement_condition']
            if column_logical_condition == "and":
                if column_judgement_condition == "=":
                    self.and_equal(column_name, column_value)
                elif column_judgement_condition == "!=":
                    self.and_unequal(column_name, column_value)
                elif column_judgement_condition == "<=":
                    self.and_less(column_name, column_value)
                elif column_judgement_condition == ">=":
                    self.and_equal_greater(column_name, column_value)
                elif column_judgement_condition == "<":
                    self.and_less(column_name, column_value)
                elif column_judgement_condition == ">":
                    self.and_greater(column_name, column_value)
                elif column_judgement_condition == "like":
                    self.and_like(column_name, column_value)
                elif column_judgement_condition == "in":
                    self.and_in(column_name, column_value)
                elif column_judgement_condition == "not in":
                    self.and_not_in(column_name, column_value)
                elif column_judgement_condition == "is null":
                    self.and_is_null(column_name)
                elif column_judgement_condition == "is not null":
                    self.and_is_not_null(column_name)
            elif column_logical_condition == "or":
                if column_judgement_condition == "=":
                    self.or_equal(column_name, column_value)
                elif column_judgement_condition == "!=":
                    self.or_unequal(column_name, column_value)
                elif column_judgement_condition == "<=":
                    self.or_less(column_name, column_value)
                elif column_judgement_condition == ">=":
                    self.or_equal_greater(column_name, column_value)
                elif column_judgement_condition == "<":
                    self.or_less(column_name, column_value)
                elif column_judgement_condition == ">":
                    self.or_greater(column_name, column_value)
                elif column_judgement_condition == "like":
                    self.or_like(column_name, column_value)
                elif column_judgement_condition == "in":
                    self.or_in(column_name, column_value)
                elif column_judgement_condition == "not in":
                    self.or_not_in(column_name, column_value)
                elif column_judgement_condition == "is null":
                    self.or_is_null(column_name)
                elif column_judgement_condition == "is not null":
                    self.or_is_not_null(column_name)
        return self

    def _build_where_clause(self):
        if len(self._or_where_clauses) == 0 and len(self._and_where_clauses) != 0:
            return " AND ".join(self._and_where_clauses)
        elif len(self._and_where_clauses) == 0 and len(self._or_where_clauses) != 0:
            return " OR ".join(self._or_where_clauses)
        elif len(self._and_where_clauses) != 0 and len(self._or_where_clauses) != 0:
            return " AND ".join(self._and_where_clauses) + " OR " + " OR ".join(self._or_where_clauses)
        else:
            return ""

class ConditionsBuilder(SQLConditionsBuilderBass):
    def __init__(self, head_sql, cursor, connect):
        super().__init__(head_sql, cursor, connect)

    def run(self):
        where_clause = self._build_where_clause()
        where_clause = f"WHERE {where_clause}" if where_clause else ""
        sql = f"{self._head_sql} {where_clause}"
        self._cursor.execute(sql)
        row = self._cursor.fetchall()
        self._connect.commit()
        self._cursor.close()
        self._connect.close()
        return row


class SelectConditionsBuilder(SQLConditionsBuilderBass):
    def __init__(self, head_sql, cursor, connect):
        super().__init__(head_sql, cursor, connect)
        self._sort_sql = ''
        self._group_by_sql = ""
        self._limit_sql = ""

    def sort(self, column: str, direction: str):
        if direction != "ASC" or direction != "DESC" or direction != "desc" or direction != "asc":
            raise ValueError("Sort direction must be ASC or DESC")
        self._sort_sql += f" ORDER BY {column} {direction}"
        return self

    def group_by(self, column: list):

        self._group_by_sql += f" GROUP BY {','.join(column)}"
        return self

    def limit(self, index: int, size: int):
        if type(index) is not int or type(size) is not int:
            raise TypeError(f'{index}, {size} type must be int')
        self._limit_sql += f" LIMIT {size} OFFSET {size * index}"
        return self

    def run(self):
        where_clause = self._build_where_clause()
        where_clause = f"WHERE {where_clause}" if where_clause else ""
        if self._sort_sql != "":
            sql = f"{self._head_sql} {where_clause}" + self._sort_sql + self._limit_sql + ";"
        elif self._group_by_sql != "":
            sql = f"{self._head_sql} {where_clause}" + self._group_by_sql + self._limit_sql + ";"
        else:
            sql = f"{self._head_sql} {where_clause}" + self._limit_sql + ";"
        self._cursor.execute(sql)
        row = self._cursor.fetchall()
        self._connect.commit()
        self._cursor.close()
        self._connect.close()
        return row


class SQLServerSelectConditionsBuilder(SelectConditionsBuilder):
    def __init__(self, head_sql, cursor, connect, columns):
        super().__init__(head_sql, cursor, connect)
        self._sort_sql = ''
        self.columns = columns
        self._group_by_sql = ""
        self._limit_sql = ""

    def limit(self, index: int, size: int):
        if type(index) is not int or type(size) is not int:
            raise TypeError(f'{index}, {size} type must be int')
        self._limit_sql += f" OFFSET {size * index} ROWS FETCH NEXT {size} ROWS ONLY;"
        return self

    def run(self):
        where_clause = self._build_where_clause()
        where_clause = f"WHERE {where_clause}" if where_clause else ""
        if self._sort_sql != "":
            sql = f"{self._head_sql} {where_clause}" + self._sort_sql + self._limit_sql + ";"
        elif self._group_by_sql != "":
            sql = f"{self._head_sql} {where_clause}" + self._group_by_sql + self._limit_sql + ";"
        else:
            sql = f"{self._head_sql} {where_clause}" + f"ORDER BY {self.columns[0]} " + self._limit_sql + ";"
        self._cursor.execute(sql)
        row = self._cursor.fetchall()
        self._connect.commit()
        self._cursor.close()
        self._connect.close()
        return row


class OracleSelectConditionsBuilder(SQLServerSelectConditionsBuilder):
    def __init__(self, head_sql, cursor, connect, columns):
        super().__init__(head_sql, cursor, connect, columns)
        self._sort_sql = ''
        self.columns = columns
        self._group_by_sql = ""
        self._limit_sql = ""

    def run(self):
        where_clause = self._build_where_clause()
        where_clause = f"WHERE {where_clause}" if where_clause else ""
        if self._sort_sql != "":
            sql_ = f"{self._head_sql} {where_clause}" + self._sort_sql + self._limit_sql
        elif self._group_by_sql != "":
            sql_ = f"{self._head_sql} {where_clause}" + self._group_by_sql + self._limit_sql
        else:
            sql_ = f"{self._head_sql} {where_clause}" + f"ORDER BY {self.columns[0]} " + self._limit_sql
        sql = sql_.replace(";", "")
        self._cursor.execute(sql)
        row = self._cursor.fetchall()
        self._connect.commit()
        self._cursor.close()
        self._connect.close()
        return row


class MySQLCreateTable:
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
            if "text" in column['type']:
                definition = f"`{column['name']}` {column['type']}"
            else:
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
        self._cursor_.close()
        self._connect_.close()


class MariaDBCreateTable(MySQLCreateTable):
    def __init__(self, connect, cursor, table_name: str, table_comment: str = None):
        super().__init__(connect, cursor, table_name, table_comment)


class SqliteCreateTable:
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


class SQLServerCreateTable:
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

        def unique(self, column_unique=False):
            """
            添加字段的唯一值约束
            :param column_unique:
            :return:
            """
            self._column_["unique"] = column_unique
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


class PostgresqlCreateTable:
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
            definition = f"{column['name']} {column['type']}({column['length']})"
            if column['null']:
                definition += " NOT NULL"
            if column['primary_key']:
                definition += " PRIMARY KEY"
            if column['auto_increment']:
                definition += " SERIAL"
            if 'comment' in column and column['comment']:
                definition += f" COMMENT '{column['comment']}'"
            column_definitions.append(definition)
        self._sql_ += ",\n".join(column_definitions)
        self._sql_ += "\n)"
        if self._table_comment_ is not None:
            self._sql_ += f"COMMENT='{self._table_comment_}'"
        self._cursor_.execute(self._sql_)
        self._connect_.commit()
