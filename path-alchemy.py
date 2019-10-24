import sqlalchemy

class PathAlchemy:

    def _get_columns(self, rs):
        columns = []
        for column in rs.cursor.description:
            columns.append(column.name)
        return columns    

    def _get_tables(self, rs, con):
        tables = {}
        for column in rs.cursor.description:
            table_name = None
            if hasattr(column, 'table_oid'):
                table_oids = {}
                if column.table_oid in table_oids:
                    table_name = table_oids[column.table_oid]
                else:
                    statement = sqlalchemy.sql.text("""select relname from pg_class where oid=:oid""")
                    table_name = con.execute(statement, {"oid":column.table_oid}).fetchone()[0]
                    table_oids[column.table_oid] = table_name
            tables[column.name] = table_name
        return tables   
    
    def _get_path_columns(self, columns, tables):
        path_columns = []
        tablecount = len(set(tables.values()))
        for i, column in enumerate(columns):
            if (column[0:1] != '$'):
                if tablecount>1:
                    path_columns.append('$[].' + tables[columns[i]] + '.' + column)
                else:
                    path_columns.append('$[].' + column)
            else:
                path_columns.append(column)
        return path_columns

    def q(self, sql, args):
        engine = sqlalchemy.create_engine('postgresql+psycopg2://php-crud-api:php-crud-api@127.0.0.1:5432/php-crud-api')
        with engine.connect() as con:

            statement = sqlalchemy.sql.text(sql)

            rs = con.execute(statement, args)

            columns = self._get_columns(rs)
            tables = self._get_tables(rs,con)
            path_columns = self._get_path_columns(columns, tables)
                
            for row in rs:
                for i, value in enumerate(row):
                    print(path_columns[i] + '=' + str(value))

p = PathAlchemy()
p.q("""SELECT * from posts where posts.id=:id""",{"id":1})
print('')
p.q("""SELECT * from posts,comments where comments.post_id = posts.id and posts.id=:id""",{"id":1})
