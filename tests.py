# coding: utf-8

import MySQLdb

from nose.tools import with_setup

from autumn import Query, Model, _default_table_name


data = [
    (1, "John", 25),
    (2, "John", 30),
    (3, "Bob", 30),
]


def init_database():
    db = MySQLdb.connect()
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS `test`")
    db.commit()
    db.close()

# Create test database
init_database()


database = MySQLdb.connect(db="test")


def setup_database():
    cursor = database.cursor()
    cursor.execute("DROP TABLE IF EXISTS `user`")
    cursor.execute(
        "CREATE TABLE `user` (`id` int PRIMARY KEY AUTO_INCREMENT, `name` varchar(100), `age` int)")
    cursor.execute(
        "INSERT INTO `user` (`id`, `name`, `age`) VALUES {}".format(", ".join(["(%s, %s, %s)"]*len(data))),
        (x for t in data for x in t))

    database.commit()


def clear_database():
    cursor = database.cursor()
    cursor.execute("DROP TABLE IF EXISTS `user`")

    database.commit()


class ModelMock(tuple):
    primary_key = "id"
    def __new__(cls, *values):
        return super(ModelMock, cls).__new__(cls, values)


class UserMock(ModelMock):
    database = database
    table_name = "user"
    def __init__(self, *values):
        setattr(self, self.primary_key, values[0])


def test_query_init():
    q = Query(model=UserMock)
    assert q._model == UserMock
    assert q._db == database


@with_setup(setup_database)
def test_query_where():
    result = data[1:2]
    assert list(Query(model=UserMock).where(name="John", age=30)) == result
    assert list(Query(model=UserMock).where(name="John").where(age=30)) == result
    assert list(Query(model=UserMock).where("name=%s and age=%s", "John", 30)) == result

    result = data[1:]
    assert list(Query(model=UserMock).where("age > 29")) == result

    # Cover condition params being None case
    result = [(4, None, 15)]
    cursor = database.cursor()
    cursor.execute("INSERT INTO `user` (`age`) VALUES (15)")
    database.commit()
    assert list(Query(model=UserMock).where(name=None)) == result


@with_setup(setup_database)
def test_query_order_by():
    result = list(reversed(data[:2]))
    assert list(Query(model=UserMock).where(name="John").order_by("id desc")) == result
    assert list(Query(model=UserMock).where(name="John").order_by("age desc")) == result


@with_setup(setup_database)
def test_query_fetch_result_slice():
    assert Query(model=UserMock)[:] == data
    assert Query(model=UserMock)[:2] == data[:2]
    assert Query(model=UserMock)[1:2] == data[1:2]

    assert Query(model=UserMock)[0] == data[0]
    assert Query(model=UserMock)[2] == data[2]
    assert Query(model=UserMock)[-1] == data[-1]
    assert Query(model=UserMock)[-2] == data[-2]

    # Cover slice start > stop case
    assert Query(model=UserMock)[2:1] == [], Query(model=UserMock)[2:1]

    # Cover getting items from cache case
    q = Query(model=UserMock)
    assert q[2:1] == []
    assert q[:] == []


@with_setup(setup_database)
def test_query_fetch_result_iterator():
    for row, result in zip(list(Query(model=UserMock)), data):
        assert row == result


@with_setup(setup_database)
def test_query_count():
    assert Query(model=UserMock).where(name="John").count() == 2
    assert Query(model=UserMock).where(name="John").count("distinct(`name`)") == 1


@with_setup(setup_database)
def test_query_delete():
    assert Query(model=UserMock).where(name="Bob").count() == 1
    assert Query(model=UserMock).where(name="Bob").delete() == 1
    assert Query(model=UserMock).where(name="Bob").count() == 0


# TODO: Test various fields type, especially DATETIME, DECIMAL.


def test_default_table_name():
    assert _default_table_name("Table") == "table"
    assert _default_table_name("Tablename") == "tablename"
    assert _default_table_name("TableName") == "table_name"


setup_database()
class User(Model):
    database = database


@with_setup(setup_database)
def test_model_init():
    assert User.table_name == "user"
    assert User.primary_key == "id"
    assert User._fields == ("id", "name", "age")


@with_setup(setup_database)
def test_model_create():
    result = (4, "Tom", 50)
    u = User(*result).save()
    assert (u.id, u.name, u.age) == result
    assert list(Query(model=UserMock).where(id=4, name="Tom")) == [result]

    result = (5, "May", 22)
    u = User(name="May", id=5, age=22).save()
    assert (u.id, u.name, u.age) == result
    assert list(Query(model=UserMock).where(id=5, name="May")) == [result]

    result = (6, "Paul", 65)
    u = User(age=65, name="Paul").save()
    assert (u.id, u.name, u.age) == result
    assert list(Query(model=UserMock).where(id=6, name="Paul")) == [result]

    # "utf8mb4" character set required to properly store emoji.
    # Article below provides backgrounds and detailed migration instruction,
    # better take a look.
    # https://mathiasbynens.be/notes/mysql-utf8mb4
    result = (7, "😜", 27)
    u = User(age=27, name="😜").save()
    assert (u.id, u.name, u.age) == result
    assert list(Query(model=UserMock).where(id=7, name="😜")) == [result]


@with_setup(setup_database)
def test_model_read():
    assert User.get(1).id == 1
    assert User.get(id=1).id == 1
    assert User.get(name="Bob").id == 3

    assert User.where(id=1)[0].id == 1
    assert User.where(name="Bob")[0].id == 3


@with_setup(setup_database)
def test_model_update():
    u = User.get(1); u.name = "Johnson"; u.save()
    assert u.name == "Johnson"
    assert User.get(1).name == "Johnson"

    u = User.get(2); u.update(name="Johnson")
    assert u.name == "Johnson"
    assert User.get(2).name == "Johnson"


@with_setup(setup_database)
def test_model_update_primary_key():
    result = (4, 4, "John", 25)
    u = User.get(1); u.id = 4; u.save()
    assert (u.id, u._pk, u.name, u.age) == result
    u = User.get(4)
    assert (u.id, u._pk, u.name, u.age) == result

    result = (5, 5, "John", 30)
    u = User.get(2); u.update(id=5)
    assert (u.id, u._pk, u.name, u.age) == result
    u = User.get(5)
    assert (u.id, u._pk, u.name, u.age) == result


@with_setup(setup_database)
def test_model_delete():
    User.get(1).delete()
    User.get(2).delete()
    assert User.get(1) is None
    assert User.get(2) is None
    assert list(Query(model=UserMock).where(id=1)) == []
    assert list(Query(model=UserMock).where(id=2)) == []


@with_setup(setup_database)
def test_model_pickle():
    import cPickle
    x = cPickle.loads(cPickle.dumps(User.get(1)))
    x.delete()
    assert (x.id, x._pk, x.name, x.age) == (1, 1, "John", 25), (x.id, x._pk, x.name, x.age)
    assert list(Query(model=UserMock).where(id=1)) == []


@with_setup(setup_database)
def test_model_default_values():
    class User(Model):
        database = database
        name = "John Doe"

    u = User().save()
    assert (u.name, u.age) == ("John Doe", None)
    assert list(Query(model=UserMock).where(id=4)) == [(4, "John Doe", None)]


@with_setup(setup_database)
def test_autumn_coverage():
    # Cover unimportant lines to simplify coverage report.

    # SQL execution exceptions handling
    try:
        Query.execute(db=database, query="syntax error oh yeah")
    except:
        pass

    # L181
    # Stop iteration of Cursor object at None to avoid AttributeError, maybe pointless
    # No idea

    q = Query(model=UserMock)

    # Query's __repr__() method
    repr(q)

    # Count Query results using cache
    q.count()

    # Early return condition check of Model's get() classmethod
    Model.get()

    # Early return condition check of Model's _update() instancemethod
    User()._update()
