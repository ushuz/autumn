# coding: utf-8

from __future__ import print_function

"""
autumn.py

A simple MySQL ORM, provides basic ORM functionalities in a Pythonic way.
"""


__version__ = "0.1.0"
__author__ = "ushuz"


import datetime


class Query(object):
    """
    SQL queries made easy. Designed specifically to work with `Model`.

    Create a `Query` instance

        >>> q = Query(model=User)

    Set `WHERE`

        >>> q.where(name="John", age=30)
        >>> q.where(name="John").where(age=30)
        >>> q.where("name = %s and age = %s", "John", 30)

    Set `ORDER BY`, `ASC` by default

        >>> q.order_by("`id` DESC")

    Set `LIMIT` and fetch results by slice

        >>> users = Query(model=User).where(name="John")[:10]   # LIMIT 0, 10
        >>> users = Query(model=User).where(name="John")[10:20] # LIMIT 10, 10

        >>> user = Query(model=User).where(name="John")[0]     # LIMIT 0, 1

    Results can also be fetched by `list`

        >>> users = list(Query(model=User).where(name="John"))

    Or by iteration

        >>> for o in Query(model=User).where(name="John"):
        ...    print("Hello", o.name)
        ...
        Hello John

    The query won't actually execute until results are fetched using slice,
    `list` or iteration.

    As MySQL doesn't support offset without limit, to better perserve Python
    semantics, queries like below will fetch **ALL** results first, put them
    into a list and then apply slice, be **CAREFUL** if you are dealing with
    a large dataset

        >>> users = Query(model=User).where(name="John")[5:]

    As MySQL

        >>> users = Query(model=User).where(name="John")[-1]

    Counting results

        >>> Query(model=User).where(name="John").count()
        2
        >>> Query(model=User).where(name="John").count("distinct(name)")
        1

    If the results have not been fetched yet, `count()` will execute a SQL query
    `SELECT COUNT(0)`. Otherwise, it will return the length of results using
    `len()`.

    Execute raw SQL

        >>> db = MySQLdb.connect(db="user")
        >>> query = "SELECT * FROM `user` WHERE id = %s"
        >>> values = (1,)
        >>> Query.execute(db=db, query=query, values=values)
    """

    def __init__(self, model, operation="SELECT *"):
        self._op = operation

        self._condition_literals = []
        self._condition_params = []

        self._order_by = ""
        self._limit = ()

        self._cache = None
        self._model = model
        self._db = model.database

    def __getitem__(self, key):
        if self._cache is not None:
            return self._cache[key]

        if isinstance(key, (int, long)):
            # If key < 0, Query should return a result from bottom. For instance,
            # -1 for the last result, -3 for the third result from bottom. Generally,
            # it's done by ORDER BY and LIMIT. But Query has no idea about any fields
            # it can apply ORDER BY to, so it just count the number of the matching
            # rows, then fetch the result from top to bottom.
            if key < 0:
                key += self.count()
            self._limit = (key, 1)
            results = self._results
            return results and results[0]

        elif isinstance(key, slice):
            if key.stop is None:
                self._limit = ()
                return self._results[key]

            if key.start is None:
                self._limit = (0, key.stop)
            elif key.start <= key.stop:
                self._limit = (key.start, key.stop - key.start)
            elif key.start > key.stop:
                self._limit = (0,)

            return self._results

    def __len__(self):
        return len(self._results)

    def __iter__(self):
        return iter(self._results)

    def __repr__(self):
        return repr(self._results)

    @classmethod
    def execute(cls, db, query, values=()):
        print("SQL:", query, values)
        cursor = db.cursor()
        try:
            cursor.execute(query, values)
            db.commit()
        except Exception as e:
            print(e)
            db.rollback()
            raise

        return cursor

    @property
    def _where_condition(self):
        return "WHERE {}".format(" AND ".join(self._condition_literals)) \
            if self._condition_literals else ""

    @property
    def _query(self):
        return "{} FROM `{}` {} {} {}".format(
            self._op,
            self._model.table_name,
            self._where_condition,
            self._order_by,
            "LIMIT {}".format(", ".join(str(x) for x in self._limit)) \
                if self._limit else "")

    @property
    def _results(self):
        if self._cache is None:
            self._cache = list(self._generator())
        return self._cache

    def _generator(self):
        cursor = Query.execute(db=self._db, query=self._query,
                               values=self._condition_params)
        for row in cursor:
            if row is None:
                break
            o = self._model(*row)
            o._is_new_record = False
            yield o

    def where(self, *args, **kwargs):
        if args:
            self._condition_literals.append(args[0])
            self._condition_params.extend(args[1:])

        for k, v in kwargs.iteritems():
            if v is None:
                self._condition_literals.append("`{}` is NULL".format(k))
                continue
            self._condition_literals.append("`{}` = %s".format(k))
            self._condition_params.append(v)

        return self

    def order_by(self, order_by):
        self._order_by = "ORDER BY {}".format(order_by)
        return self

    def count(self, what="0"):
        if (what == "0" or what == "*") and self._cache is not None:
            return len(self._cache)

        cursor = Query.execute(
            db=self._db,
            query="SELECT COUNT({}) FROM `{}` {}".format(
                what, self._model.table_name, self._where_condition),
            values=self._condition_params)

        return cursor.fetchone()[0]

    def delete(self):
        cursor = Query.execute(
            db=self._db,
            query="DELETE FROM `{}` {}".format(
                self._model.table_name, self._where_condition),
            values=self._condition_params)

        return cursor.rowcount


def _default_table_name(name):
    """
    Translate `MyModel` to `my_model`.
    """
    return reduce(
        lambda x, y: "_".join((x, y)) if y.isupper() else "".join((x, y)),
        list(name)).lower()


class ModelMetaclass(type):
    """
    Metaclass for Model.

    Setup meta for the model, like fields, default table name and primary key, etc.
    """
    def __new__(cls, name, bases, attrs):
        model = super(ModelMetaclass, cls).__new__(cls, name, bases, attrs)

        if name == "Model":
            return model

        assert getattr(model, "database")

        model.table_name = attrs.get("table_name", _default_table_name(name))
        model.primary_key = attrs.get("primary_key", "id")

        database = model.database
        cursor = database.cursor()
        cursor.execute("SELECT * FROM `{}` LIMIT 0".format(model.table_name))
        database.commit()

        model._fields = tuple([f[0] for f in cursor.description])
        model._field_types = {f: type(attrs.get(f)) for f in model._fields}

        assert hasattr(model, "table_name")
        assert hasattr(model, "primary_key")
        assert hasattr(model, "_fields")

        return model


class Model(object):
    """
    All models are taken care of, simple and stupid.

        class MyModel(Model):
            # Model meta
            database = MySQLdb.connect(db="database")
            table_name = "my_model"

            # Fields default values
            field = 1
            another_field = "very string"

    Create

        >>> m = MyModel(1, "very string")
        >>> m = MyModel(field=1, another_field="very string")
        >>> m.save()
        <__main__.MyModel object at 0x106428090>

    Read

        >>> m = MyModel.get(1)              # get by `id`
        >>> m = MyModel.get(field=1)        # get by `field`
        >>> m = MyModel.get(another_field="very string")

    Update

        >>> m.field = 123
        >>> m.save()
        <__main__.MyModel object at 0x106428090>

    Delete

        >>> m.delete()

    Slices are translated into the `LIMIT` of SQL queries, and then return a
    list of MyModel instances.

        >>> m = MyModel.where()[:5]    # LIMIT 0, 5
        >>> m = MyModel.where()[10:15] # LIMIT 10, 5

    We can fetch all instances of a Model by slice, built-in `list` function
    and iteration.

        >>> m = MyModel.where()[:]
        >>> m = list(MyModel.where(name="John").where("age < %s", 18))
        >>> for m in MyModel.where():
        ...    # do something here...

    `where` returns a Query object, and we can `where` it again. The query
    won't actually execute until trying to fetch any results from it.

        >>> m = MyModel.where(field=1).where(another_field=2)
        >>> m = MyModel.where(field=1, another_field=2)

    We can add `ORDER BY` to SQL queries by `order_by`.

        >>> m = MyModel.where(field=1).order_by("id DESC")
    """

    __metaclass__ = ModelMetaclass

    def __init__(self, *args, **kwargs):
        self._pk_value = None
        self._is_new_record = True
        self._changed_fields = set()

        # Set attributes by arguments passed in column order
        for i, arg in enumerate(args[:len(self._fields)]):
            self.__dict__[self._fields[i]] = arg

        # Set attributes by keyword arguments
        for i in self._fields[len(args):]:
            self.__dict__[i] = kwargs.get(i)

        # Set primary key value
        self._pk_value = getattr(self, self.primary_key, None)

    def __getstate__(self):
        """Save every fields of an instance into a tuple."""
        value = []
        for i in self._fields:
            v = getattr(self, i, None)
            value.append(v)
        return tuple(value)

    def __setstate__(self, value):
        """Re-build an instance from a tuple."""
        self.__init__(*value)
        self._is_new_record = False

    def __setattr__(self, name, value):
        """Set attributes and save changed fields into a set."""
        # TODO: Ensure value type are the same as field type.
        if name in self._fields:
            self._changed_fields.add(name)
        super(Model, self).__setattr__(name, value)

    @classmethod
    def get(cls, pk=None, **kwargs):
        """Return the first result fetched."""
        if pk is None and not kwargs:
            return

        if pk is not None:
            kwargs = { cls.primary_key: pk }

        q = Query(model=cls).where(**kwargs)[:1]
        if q:
            ins = q[0]
            ins._is_new_record = False
            return ins

    @classmethod
    def where(cls, *args, **kwargs):
        return Query(model=cls).where(*args, **kwargs)

    @property
    def _pk(self):
        return self._pk_value

    @_pk.setter
    def _pk(self, value):
        setattr(self, "_pk_value", value)
        setattr(self, self.primary_key, value)

    def _set_default_values(self):
        """Set attributes to their default values if not been set."""
        for k, v in self.__class__.__dict__.iteritems():
            # If model property name is
            if k not in self._fields:
                continue
            if getattr(self, k, None) is None:
                v = v() if callable(v) else v
                setattr(self, k, v)

    def _insert(self):
        """Insert the record.

        If the value of primary key is specified, like `id`, then insert it into
        database as well. Otherwise, take `lastrowid` as the value of primary
        key.
        """
        fields = self._fields[:]
        used_fields = []
        values = []

        for f in fields:
            v = getattr(self, f, None)
            if v is not None:
                used_fields.append(f)
                values.append(v)

        query = "INSERT INTO `{}` ({}) VALUES ({})".format(
                self.table_name,
                ", ".join(("`{}`".format(f) for f in used_fields)),
                ", ".join(("%s",) * len(used_fields)))

        cursor = Query.execute(db=self.database, query=query, values=values)

        if getattr(self, self.primary_key, None) is None:
            self._pk = cursor.lastrowid

    def _update(self):
        """Update the record."""
        if not self._changed_fields:
            return

        self.before_update()

        query = "UPDATE `{}` SET {} {}".format(
            self.table_name,
            ",".join(("`{}` = %s".format(f) for f in self._changed_fields)),
            "WHERE `{}` = %s".format(self.primary_key))

        values = [getattr(self, f) for f in self._changed_fields]
        values.append(self._pk)

        Query.execute(db=self.database, query=query, values=values)

        # Update primary key value after the execution of a query as it maybe changed
        self._pk = getattr(self, self.primary_key)

        self.after_update()

    def save(self):
        if self._is_new_record:
            self._set_default_values()
            self._insert()
            self._is_new_record = False

        elif self._changed_fields:
            self._update()

        self._changed_fields.clear()

        return self

    def delete(self):
        """Delete the record."""
        self.before_delete()

        query = "DELETE FROM `{}` WHERE `{}` = %s".format(
            self.table_name, self.primary_key)
        values = (self._pk,)

        Query.execute(db=self.database, query=query, values=values)

        self.after_delete()

    def update(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        self.save()

    def before_update(self):
        pass

    def after_update(self):
        pass

    def before_delete(self):
        pass

    def after_delete(self):
        pass
