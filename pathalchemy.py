from sqlalchemy import create_engine, sql
from collections import OrderedDict 
from hashlib import md5
from json import JSONEncoder

class PathError(Exception):
   """Class for PathAlchemy path exceptions"""
   pass

class PathAlchemy:

    _engine = None

    def __init__(self, dsn):
        self._engine = create_engine(dsn)

    def _get_columns(self, rs):
        columns = []
        for column in rs.cursor.description:
            columns.append(column.name)
        return columns    

    def _get_tables(self, rs, con):
        tables = []
        for column in rs.cursor.description:
            table_name = None
            if hasattr(column, 'table_oid'):
                table_oids = {}
                if column.table_oid in table_oids:
                    table_name = table_oids[column.table_oid]
                else:
                    statement = sql.text("""select relname from pg_class where oid=:oid""")
                    result = con.execute(statement, {"oid":column.table_oid}).fetchone()
                    if result != None:
                        table_name = result[0]
                    table_oids[column.table_oid] = table_name
            tables.append(table_name)
        return tables   
    
    def _get_table_count(self, tables):
        table_set = set(tables)
        table_set.discard(None)
        return len(table_set)

    def _get_paths(self, columns, tables):
        paths = []
        tablecount = self._get_table_count(tables)
        for i, column in enumerate(columns):
            if (column[0:1] != '$'):
                if tablecount>1 and tables[i]!=None:
                    paths.append('$[].' + tables[i] + '.' + column)
                else:
                    paths.append('$[].' + column)
            else:
                paths.append(column)
        return paths

    def _get_meta(self,rs,con):
        columns = self._get_columns(rs)
        tables = self._get_tables(rs,con)
        paths = self._get_paths(columns, tables)
        meta = []
        for i, column in enumerate(columns):
            meta.append({'name':column,'table':tables[i],'path':paths[i]})
        return meta

    def q(self, query, args={}):
        with self._engine.connect() as con:
            statement = sql.text(query)
            rs = con.execute(statement, args)
            meta = self._get_meta(rs,con)
            records = self._get_all_records(rs, meta)
            results = self._group_by_separator(records,'[]')
            results = self._add_hashes(results)
            results = self._combine_into_tree(results,'.')
            results = self._remove_hashes(results)
            return results

    def _get_all_records(self, rs, meta):
        records = []
        for row in rs:
            record = OrderedDict()
            for i, value in enumerate(row):
                record[meta[i]['path'][1:]] = value
            records.append(record)
        return records

    def _group_by_separator(self, records, separator):
        results = []
        for record in records:
            result = OrderedDict()
            for name,value in record.items():
                parts = name.split(separator)
                newName = parts.pop()
                path = separator.join(parts)
                if len(parts)>0:
                    path += separator
                if not path in result:
                    result[path] = OrderedDict()
                result[path][newName] = value
            results.append(result)
        return results
    
    def _add_hashes(self, records):
        results = []
        for record in records:
            mapping = OrderedDict()
            for key, part in record.items():
                if key[-2:]!='[]':
                    continue
                encoder = JSONEncoder(ensure_ascii=False,separators=(',',':'))
                hash = md5(encoder.encode(part).encode('utf-8')).hexdigest()
                mapping[key] = key[:-2] + '.!' + hash + '!'
            newKeys = []
            for key in record.keys():
                for search in sorted(mapping.keys(), key=len, reverse=True):
                    key = key.replace(search, mapping[search])
                newKeys.append(key)
            results.append(OrderedDict(zip(newKeys, record.values())))
        return results

    def _combine_into_tree(self, records, separator):
        results = OrderedDict()
        for record in records:
            for name, value in record.items():
                for key, v in value.items():
                    path = (name + key).split(separator)
                    newName = path.pop()
                    current = results
                    for p in path:
                        if not p in current:
                            current[p] = OrderedDict()
                        current = current[p]
                    current[newName] = v
        return results['']

    def _remove_hashes(self, tree, path='$'):
        values = OrderedDict()
        trees = OrderedDict()
        results = []
        for key, value in tree.items():
            if type(value) == OrderedDict:
                if key[:1] == '!' and key[-1:] == '!':
                    results.append(self._remove_hashes(tree[key],path+'[]'))
                else:
                    trees[key] = self._remove_hashes(tree[key],path+'.'+key)
            else:
                values[key] = value
        if len(results):
            hidden = list(values.keys()) + list(trees.keys())
            if len(hidden)>0:
                raise PathError('Path "'+path+'.'+hidden[0]+'" is hidden by path "'+path+'[]"')
            return results
        return OrderedDict(list(values.items()) + list(trees.items()))
    
    @staticmethod
    def create(username, password, database, address='127.0.0.1', port='5432'):
        return PathAlchemy('postgresql+psycopg2://'+username+':'+password+'@'+address+':'+port+'/'+database)
