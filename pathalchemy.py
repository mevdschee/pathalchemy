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

    def _get_columns(self, descripton):
        columns = []
        for column in descripton:
            if hasattr(column, "name"):
                columns.append(column.name)
            else:
                columns.append(column[0])
        return columns

    def _get_paths(self, columns):
        paths = []
        path = "$[]"
        for column in columns:
            prop = column
            if column[0:1] == "$":
                pos = column.rfind(".")
                if pos != -1:
                    path = column[:pos]
                    prop = column[pos + 1 :]
            paths.append(path + "." + prop)
        return paths

    def path_query(self, query, params={}):
        with self._engine.connect() as con:
            statement = sql.text(query)
            rs = con.execute(statement, params)
            columns = self._get_columns(rs.cursor.description)
            paths = self._get_paths(columns)
            records = self._get_all_records(rs, paths)
            groups = self._group_by_separator(records, "[]")
            hashes = self._add_hashes(groups)
            tree = self._combine_into_tree(hashes, ".")
            result = self._remove_hashes(tree, "$")
            return result

    def _get_all_records(self, rs, paths):
        records = []
        for row in rs:
            record = OrderedDict()
            for i, value in enumerate(row):
                record[paths[i][1:]] = value
            records.append(record)
        return records

    def _group_by_separator(self, records, separator):
        results = []
        for record in records:
            result = OrderedDict()
            for name, value in record.items():
                parts = name.split(separator)
                newName = parts.pop()
                path = separator.join(parts)
                if len(parts) > 0:
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
                if key[-2:] != "[]":
                    continue
                encoder = JSONEncoder(ensure_ascii=False, separators=(",", ":"))
                hash = md5(encoder.encode(part).encode("utf-8")).hexdigest()
                mapping[key] = key[:-2] + ".!" + hash + "!"
            mappingKeys = sorted(mapping.keys(), key=len, reverse=True)
            result = {}
            for key, value in record.items():
                for search in mappingKeys:
                    key = key.replace(search, mapping[search])
                result[key] = value
            results.append(result)
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
        return results[""]

    def _remove_hashes(self, tree, path):
        values = OrderedDict()
        trees = OrderedDict()
        results = []
        for key, value in tree.items():
            if type(value) == OrderedDict:
                if key[:1] == "!" and key[-1:] == "!":
                    results.append(self._remove_hashes(tree[key], path + "[]"))
                else:
                    trees[key] = self._remove_hashes(tree[key], path + "." + key)
            else:
                values[key] = value
        if len(results):
            hidden = list(values.keys()) + list(trees.keys())
            if len(hidden) > 0:
                raise PathError(
                    'The path "%s.%s" is hidden by the path "%s[]"'
                    % (path, hidden[0], path)
                )
            return results
        return OrderedDict(list(values.items()) + list(trees.items()))

    @staticmethod
    def create(username, password, database, driver="postgresql", address="127.0.0.1", port="5432"):
        return PathAlchemy(
            "%s://%s:%s@%s:%s/%s"
            % (driver, username, password, address, port, database)
        )
