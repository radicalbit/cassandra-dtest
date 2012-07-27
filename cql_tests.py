# coding: utf-8

from dtest import Tester
from assertions import *
from tools import *

import os, sys, time, tools, json
from uuid import UUID
from ccmlib.cluster import Cluster

cql_version="3.0.0-beta1"

def assert_json(res, expected, col=0):
    assert len(res) == 1, res
    value = json.loads(res[0][col])
    assert value == expected, value

class TestCQL(Tester):

    def prepare(self, ordered=False):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)
        return cursor

    @since('1.1')
    def static_cf_test(self):
        """ Test static CF syntax """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        # Inserts
        cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        cursor.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee' ],
            [ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ],
        ], res

        # Test batch inserts
        cursor.execute("""
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None ],
            [ UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None ],
        ], res

    @since('1.2')
    def noncomposite_static_cf_test(self):
        """ Test non-composite static CF syntax """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        cursor.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee' ],
            [ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ],
        ], res

        # Test batch inserts
        cursor.execute("""
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None ],
            [ UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None ],
        ], res

    @since('1.1')
    def dynamic_cf_test(self):
        """ Test non-composite dynamic CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
        cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
        cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)")
        cursor.execute("UPDATE clicks SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'")
        cursor.execute("UPDATE clicks SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'")

        # Queries
        cursor.execute("SELECT url, time FROM clicks WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ 'http://bar.bar', 128 ], [ 'http://foo-2.bar', 24 ], [ 'http://foo-3', 12 ], [ 'http://foo.bar', 42 ]], res

        cursor.execute("SELECT * FROM clicks WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://bar.foo', 24 ],
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://foo-3', 12 ]
        ], res

        cursor.execute("SELECT time FROM clicks")
        res = cursor.fetchall()
        # Result from 'f47ac10b-58cc-4372-a567-0e02b2c3d479' are first
        assert res == [[24], [12], [128], [24], [12], [42]], res

    @since('1.1')
    def dense_cf_test(self):
        """ Test composite 'dense' CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE connections (
                userid uuid,
                ip text,
                port int,
                time bigint,
                PRIMARY KEY (userid, ip, port)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)")
        cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)")
        cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)")
        cursor.execute("UPDATE connections SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80")

        # Queries
        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ '192.168.0.1', 80, 42 ], [ '192.168.0.2', 80, 24 ], [ '192.168.0.2', 90, 42 ]], res

        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip >= '192.168.0.2'")
        res = cursor.fetchall()
        assert res == [[ '192.168.0.2', 80, 24 ], [ '192.168.0.2', 90, 42 ]], res

        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip = '192.168.0.2'")
        res = cursor.fetchall()
        assert res == [[ '192.168.0.2', 80, 24 ], [ '192.168.0.2', 90, 42 ]], res

        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip > '192.168.0.2'")
        res = cursor.fetchall()
        assert res == [], res

        # Deletion
        cursor.execute("DELETE time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND ip = '192.168.0.2' AND port = 80")
        cursor.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("DELETE FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        cursor.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert len(res) == 0, res

    @since('1.1')
    def sparse_cf_test(self):
        """ Test composite 'sparse' CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE timeline (
                userid uuid,
                posted_month int,
                posted_day int,
                body text,
                posted_by text,
                PRIMARY KEY (userid, posted_month, posted_day)
            );
        """)

        # Inserts
        cursor.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 12, 'Something else', 'Frodo Baggins')")
        cursor.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 24, 'Something something', 'Frodo Baggins')")
        cursor.execute("UPDATE timeline SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND posted_month = 1 AND posted_day = 3")
        cursor.execute("UPDATE timeline SET body = 'Yet one more message' WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 and posted_day = 30")

        # Queries
        cursor.execute("SELECT body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day = 24")
        res = cursor.fetchall()
        assert res == [[ 'Something something', 'Frodo Baggins' ]], res

        cursor.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day > 12")
        res = cursor.fetchall()
        assert res == [
            [ 24, 'Something something', 'Frodo Baggins' ],
            [ 30, 'Yet one more message', None ]
        ], res

        cursor.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1")
        res = cursor.fetchall()
        assert res == [
            [ 12, 'Something else', 'Frodo Baggins' ],
            [ 24, 'Something something', 'Frodo Baggins' ],
            [ 30, 'Yet one more message', None ]
        ], res

    @since('1.1')
    def create_invalid_test(self):
        """ Check invalid CREATE TABLE requests """

        cursor = self.prepare()

        assert_invalid(cursor, "CREATE TABLE test ()")
        if self.cluster.version() < "1.2":
            assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY)")
        assert_invalid(cursor, "CREATE TABLE test (c1 text, c2 text, c3 text)")
        assert_invalid(cursor, "CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)")

        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, key int)")
        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, c int, c text)")

        assert_invalid(cursor, "CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE")

    @since('1.1')
    def limit_ranges_test(self):
        """ Validate LIMIT option for 'range queries' in SELECT statements """
        cursor = self.prepare(ordered=True)

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in [ 'com', 'org', 'net' ]:
                cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (id, tld))

        # Queries
        cursor.execute("SELECT * FROM clicks WHERE userid >= 2 LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 2, 'http://foo.com', 42 ]], res

        cursor.execute("SELECT * FROM clicks WHERE userid > 2 LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 3, 'http://foo.com', 42 ]], res

    @since('1.1')
    def limit_multiget_test(self):
        """ Validate LIMIT option for 'multiget' in SELECT statements """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in [ 'com', 'org', 'net' ]:
                cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (id, tld))

        # Queries
        # That that we do limit the output to 1 *and* that we respect query
        # order of keys (even though 48 is after 2)
        cursor.execute("SELECT * FROM clicks WHERE userid IN (48, 2) LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 48, 'http://foo.com', 42 ]], res

    @since('1.1')
    def limit_sparse_test(self):
        """ Validate LIMIT option for sparse table in SELECT statements """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                day int,
                month text,
                year int,
                PRIMARY KEY (userid, url)
            );
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in [ 'com', 'org', 'net' ]:
                cursor.execute("INSERT INTO clicks (userid, url, day, month, year) VALUES (%i, 'http://foo.%s', 1, 'jan', 2012)" % (id, tld))

        # Queries
        # Check we do get as many rows as requested
        cursor.execute("SELECT * FROM clicks LIMIT 4")
        res = cursor.fetchall()
        assert len(res) == 4, res

    @since('1.1')
    def counters_test(self):
        """ Validate counter support """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        cursor.execute("UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ 1 ]], res

        cursor.execute("UPDATE clicks SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ -3 ]], res

        cursor.execute("UPDATE clicks SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ -2 ]], res

        cursor.execute("UPDATE clicks SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ -4 ]], res

    @require('#3680')
    def indexed_with_eq_test(self):
        """ Check that you can query for an indexed column even with a key EQ clause """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        cursor.execute("CREATE INDEX byAge ON users(age)")

        # Inserts
        cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        cursor.execute("SELECT firstname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("SELECT firstname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33")
        res = cursor.fetchall()
        assert res == [[ 'Samwise' ]], res

    @since('1.1')
    def select_key_in_test(self):
        """ Query for KEY IN (...) """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        # Inserts
        cursor.execute("""
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)
        """)
        cursor.execute("""
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, 'Samwise', 'Gamgee', 33)
        """)

        # Select
        cursor.execute("""
                SELECT firstname, lastname FROM users
                WHERE userid IN (550e8400-e29b-41d4-a716-446655440000, f47ac10b-58cc-4372-a567-0e02b2c3d479)
        """)

        res = cursor.fetchall()
        assert len(res) == 2, res

    @since('1.1')
    def exclusive_slice_test(self):
        """ Test SELECT respects inclusive and exclusive bounds """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        # Queries
        cursor.execute("SELECT v FROM test WHERE k = 0")
        res = cursor.fetchall()
        assert len(res) == 10, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c >= '' AND c <= ''")
        res = cursor.fetchall()
        assert len(res) == 10, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c > '' AND c < ''")
        res = cursor.fetchall()
        assert len(res) == 10, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c <= 6")
        res = cursor.fetchall()
        assert len(res) == 5 and res[0][0] == 2 and res[len(res) - 1][0] == 6, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6")
        res = cursor.fetchall()
        assert len(res) == 4 and res[0][0] == 3 and res[len(res) - 1][0] == 6, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6")
        res = cursor.fetchall()
        assert len(res) == 4 and res[0][0] == 2 and res[len(res) - 1][0] == 5, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c < 6")
        res = cursor.fetchall()
        assert len(res) == 3 and res[0][0] == 3 and res[len(res) - 1][0] == 5, res

    @since('1.1')
    def in_clause_wide_rows_test(self):
        """ Check IN support for 'wide rows' in SELECT statement """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        cursor.execute("SELECT v FROM test1 WHERE k = 0 AND c IN (5, 2, 8)")
        res = cursor.fetchall()
        assert res == [[5], [2], [8]], res

        # composites
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, 0, %i, %i)" % (x, x))

        # Check first we don't allow IN everywhere
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")

        cursor.execute("SELECT v FROM test2 WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)")
        res = cursor.fetchall()
        assert res == [[5], [2], [8]], res

    @since('1.1')
    def order_by_test(self):
        """ Check ORDER BY support in SELECT statement """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        cursor.execute("SELECT v FROM test1 WHERE k = 0 ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(9, -1, -1)], res

        # composites
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        # Inserts
        for x in range(0, 4):
            for y in range(0, 2):
                cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, %i)" % (x, y, x * 2 + y))

        # Check first we don't always ORDER BY
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c DESC")
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC")
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY k DESC")

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(7, -1, -1)], res

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1")
        res = cursor.fetchall()
        assert res == [[x] for x in range(0, 8)], res

    @since('1.1')
    def more_order_by_test(self):
        """ More ORDER BY checks (#4160) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE COLUMNFAMILY Test (
                row text,
                number int,
                string text,
                PRIMARY KEY (row, number)
            ) WITH COMPACT STORAGE
        """)

        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 1, 'one');")
        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 2, 'two');")
        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 3, 'three');")
        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 4, 'four');")

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number ASC;")
        res = cursor.fetchall()
        assert res == [[1], [2]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number ASC;")
        res = cursor.fetchall()
        assert res == [[3], [4]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[2], [1]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[4], [3]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number > 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[4]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number <= 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[3], [2], [1]], res

    @since('1.1')
    def order_by_validation_test(self):
        """ Check we don't allow order by on row key (#4246) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY (k1, k2)
            )
        """)

        q = "INSERT INTO test (k1, k2, v) VALUES (%d, %d, %d)"
        cursor.execute(q % (0, 0, 0))
        cursor.execute(q % (1, 1, 1))
        cursor.execute(q % (2, 2, 2))

        assert_invalid(cursor, "SELECT * FROM test ORDER BY k2")


    @since('1.1')
    def reversed_comparator_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC);
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        cursor.execute("SELECT v FROM test WHERE k = 0 ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(0, 10)], res

        cursor.execute("SELECT v FROM test WHERE k = 0 ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(9, -1, -1)], res

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v text,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        # Inserts
        for x in range(0, 10):
            for y in range(0, 10):
                cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, '%i%i')" % (x, y, x, y))

        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 ASC")
        res = cursor.fetchall()
        assert res == [['%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 DESC")
        res = cursor.fetchall()
        assert res == [['%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 ASC")
        res = cursor.fetchall()
        assert res == [['%i%i' % (x, y)] for x in range(9, -1, -1) for y in range(0, 10)], res

        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

    @since('1.1')
    def invalid_old_property_test(self):
        """ Check obsolete properties from CQL2 are rejected """
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp")

        cursor.execute("CREATE TABLE test (foo text PRIMARY KEY, c int)")
        assert_invalid(cursor, "ALTER TABLE test WITH default_validation=int;")

    @since('1.1')
    def alter_type_test(self):
        """ Validate ALTER TYPE behavior """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        cursor.execute("ALTER TABLE test ALTER v TYPE float")
        cursor.execute("INSERT INTO test (k, v) VALUES (0, 2.4)")

    @require('#3783')
    def null_support_test(self):
        """ Test support for nulls """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                c3 int,
                v int,
                PRIMARY KEY (k, c1, c2, c3)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 0, 0, 0)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 0, 1, 1)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 1, 0, 2)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 1, 1, 3)")

        cursor.execute("INSERT INTO test1 (k, c1, c2, v) VALUES (0, 0, 0, 10)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 1, null, 11)")

        #cursor.execute("SELECT v FROM test1 WHERE k = 0")
        #res = cursor.fetchall()
        #assert res == [[10], [0], [1], [11], [2], [3]], res

        #cursor.execute("SELECT v FROM test1 WHERE k = 0 AND c1 = 0 AND c2 = 0")
        #res = cursor.fetchall()
        #assert res == [[10], [0], [1]], res

        cursor.execute("SELECT v FROM test1 WHERE k = 0 AND c1 = 0 AND c2 = 0 AND c3 = null")
        res = cursor.fetchall()
        assert res == [[10]], res

    @require('#3680')
    def nameless_index_test(self):
        """ Test CREATE INDEX without name and validate the index can be dropped """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE users (
                id text PRIMARY KEY,
                birth_year int,
            )
        """)

        cursor.execute("CREATE INDEX on users(birth_year)")

        cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Tom', 42)")
        cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Paul', 24)")
        cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Bob', 42)")

        cursor.execute("SELECT id FROM users WHERE birth_year = 42")
        res = cursor.fetchall()
        assert res == [['Tom'], ['Bob']]

        cursor.execute("DROP INDEX users_birth_year_idx")

        assert_invalid(cursor, "SELECT id FROM users WHERE birth_year = 42")

    @since('1.1')
    def deletion_test(self):
        """ Test simple deletion and in particular check for #4193 bug """

        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE testcf (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id)
            );
        """)

        q = "INSERT INTO testcf (username, id, name, stuff) VALUES ('%s', %d, '%s', '%s');"
        row1 = ('abc', 2, 'rst', 'some value')
        row2 = ('abc', 4, 'xyz', 'some other value')
        cursor.execute(q % row1)
        cursor.execute(q % row2)

        cursor.execute("SELECT * FROM testcf")
        res = cursor.fetchall()
        assert res == [ list(row1), list(row2) ], res

        cursor.execute("DELETE FROM testcf WHERE username='abc' AND id=2")

        cursor.execute("SELECT * FROM testcf")
        res = cursor.fetchall()
        assert res == [ list(row2) ], res

        # Compact case
        cursor.execute("""
            CREATE TABLE testcf2 (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id, name)
            ) WITH COMPACT STORAGE;
        """)

        q = "INSERT INTO testcf2 (username, id, name, stuff) VALUES ('%s', %d, '%s', '%s');"
        row1 = ('abc', 2, 'rst', 'some value')
        row2 = ('abc', 4, 'xyz', 'some other value')
        cursor.execute(q % row1)
        cursor.execute(q % row2)

        cursor.execute("SELECT * FROM testcf2")
        res = cursor.fetchall()
        assert res == [ list(row1), list(row2) ], res

        # Won't be allowed until #3708 is in
        if self.cluster.version() < "1.2":
            assert_invalid(cursor, "DELETE FROM testcf2 WHERE username='abc' AND id=2")

    @since('1.1')
    def count_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE events (
                kind text,
                time int,
                value1 int,
                value2 int,
                PRIMARY KEY(kind, time)
            )
        """)

        full = "INSERT INTO events (kind, time, value1, value2) VALUES ('ev1', %d, %d, %d)"
        no_v2 = "INSERT INTO events (kind, time, value1) VALUES ('ev1', %d, %d)"

        cursor.execute(full  % (0, 0, 0))
        cursor.execute(full  % (1, 1, 1))
        cursor.execute(no_v2 % (2, 2))
        cursor.execute(full  % (3, 3, 3))
        cursor.execute(no_v2 % (4, 4))
        cursor.execute("INSERT INTO events (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")

        cursor.execute("SELECT COUNT(*) FROM events WHERE kind = 'ev1'")
        res = cursor.fetchall()
        assert res == [[5]], res

        cursor.execute("SELECT COUNT(1) FROM events WHERE kind IN ('ev1', 'ev2') AND time=0")
        res = cursor.fetchall()
        assert res == [[2]], res

    @since('1.1')
    def reserved_keyword_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                key text PRIMARY KEY,
                count counter,
            )
        """)

        assert_invalid(cursor, "CREATE TABLE test2 ( select text PRIMARY KEY, x int)")

    @since('1.1')
    def timeuuid_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE timeline (
                k text,
                time timeuuid,
                value int,
                PRIMARY KEY(k, time)
            )
        """)

        q = "INSERT INTO timeline (k, time, value) VALUES ('k', '%s', %d)"
        cursor.execute(q % ('2012-04-10', 0))
        cursor.execute(q % ('2012-04-15', 1))
        cursor.execute(q % ('2012-04-22', 2))
        cursor.execute(q % ('now', 3))

        cursor.execute("SELECT value FROM timeline WHERE k='k' AND time > '2012-04-15'")
        res = cursor.fetchall()
        assert res == [[2], [3]], res

    @since('1.1')
    def identifier_test(self):
        cursor = self.prepare()

        # Test case insensitivity
        cursor.execute("CREATE TABLE test1 (key_23 int PRIMARY KEY, CoLuMn int)")

        # Should work
        cursor.execute("INSERT INTO test1 (Key_23, Column) VALUES (0, 0)")
        cursor.execute("INSERT INTO test1 (KEY_23, COLUMN) VALUES (0, 0)")

        # Reserved keywords
        assert_invalid(cursor, "CREATE TABLE test1 (select int PRIMARY KEY, column int)")

    @since('1.1')
    def keyspace_test(self):
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE KEYSPACE test1")
        cursor.execute("CREATE KEYSPACE test2 WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1")
        assert_invalid(cursor, "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1")

        cursor.execute("DROP KEYSPACE test2")
        assert_invalid(cursor, "DROP KEYSPACE non_existing")
        cursor.execute("CREATE KEYSPACE test2 WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1")

    @since('1.1')
    def table_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                name int,
                value int,
                PRIMARY KEY(k, name)
            ) WITH COMPACT STORAGE
        """)

        cursor.execute("""
            CREATE TABLE test3 (
                k int,
                c int,
                PRIMARY KEY (k),
            )
        """)

        # existing table
        assert_invalid(cursor, "CREATE TABLE test3 (k int PRIMARY KEY, c int)")
        # repeated column
        assert_invalid(cursor, "CREATE TABLE test4 (k int PRIMARY KEY, c int, k text)")

        # compact storage limitations
        assert_invalid(cursor, "CREATE TABLE test4 (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE")

        cursor.execute("DROP TABLE test1")
        cursor.execute("TRUNCATE test2")

        cursor.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c1 int,
                c2 int,
            )
        """)

    @since('1.1')
    def batch_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE users (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        cursor.execute("""
            BEGIN BATCH USING CONSISTENCY QUORUM
                INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM users WHERE userid = 'user1';
            APPLY BATCH;
        """)

    @since('1.1')
    def token_range_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int,
                v int
            )
        """)

        c = 100
        for i in range(0, c):
            cursor.execute("INSERT INTO test (k, c, v) VALUES (%d, %d, %d)" % (i, i, i))

        cursor.execute("SELECT k FROM test")
        inOrder = [ x[0] for x in cursor.fetchall() ]
        assert len(inOrder) == c, 'Expecting %d elements, got %d' % (c, len(inOrder))

        cursor.execute("SELECT k FROM test WHERE token(k) >= '0'")
        res = cursor.fetchall()
        assert len(res) == c, "%s [all: %s]" % (str(res), str(inOrder))

        assert_invalid(cursor, "SELECT k FROM test WHERE token(k) >= 0")

        cursor.execute("SELECT k FROM test WHERE token(k) >= token(%d) AND token(k) < token(%d)" % (inOrder[32], inOrder[65]))
        res = cursor.fetchall()
        assert res == [ [inOrder[x]] for x in range(32, 65) ], "%s [all: %s]" % (str(res), str(inOrder))

    @since('1.1')
    def table_options_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int
            ) WITH comment = 'My comment'
               AND read_repair_chance = 0.5
               AND dclocal_read_repair_chance = 0.5
               AND gc_grace_seconds = 4
               AND bloom_filter_fp_chance = 0.01
               AND compaction_strategy_class = LeveledCompactionStrategy
               AND compaction_strategy_options:sstable_size_in_mb = 10
               AND compression_parameters:sstable_compression = ''
               AND caching = all
        """)

        cursor.execute("""
            ALTER TABLE test
            WITH comment = 'other comment'
             AND read_repair_chance = 0.3
             AND dclocal_read_repair_chance = 0.3
             AND gc_grace_seconds = 100
             AND bloom_filter_fp_chance = 0.1
             AND compaction_strategy_class = SizeTieredCompactionStrategy
             AND compaction_strategy_options:min_sstable_size = 42
             AND compression_parameters:sstable_compression = SnappyCompressor
             AND caching = rows_only
        """)

    @since('1.1')
    def timestamp_and_ttl_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c text
            )
        """)

        cursor.execute("INSERT INTO test (k, c) VALUES (1, 'test')")
        cursor.execute("INSERT INTO test (k, c) VALUES (2, 'test') USING TTL 400")

        cursor.execute("SELECT k, c, writetime(c), ttl(c) FROM test")
        res = cursor.fetchall()
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], (int, long))
            if r[0] == 1:
                assert r[3] == None, res
            else:
                assert isinstance(r[3], (int, long)), res

        assert_invalid(cursor, "SELECT k, c, writetime(k) FROM test")

    @since('1.1')
    def no_range_ghost_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        for k in range(0, 5):
            cursor.execute("INSERT INTO test (k, v) VALUES (%d, 0)" % k)

        cursor.execute("SELECT k FROM test")
        res = sorted(cursor.fetchall())
        assert res == [[k] for k in range(0, 5)], res

        cursor.execute("DELETE FROM test WHERE k=2")

        cursor.execute("SELECT k FROM test")
        res = sorted(cursor.fetchall())
        assert res == [[k] for k in range(0, 5) if k is not 2], res

        # Example from #3505
        cursor.execute("CREATE KEYSPACE ks1 with strategy_class = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1;")
        cursor.execute("USE ks1")
        cursor.execute("""
            CREATE COLUMNFAMILY users (
                KEY varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                birth_year bigint)
        """)

        cursor.execute("INSERT INTO users (KEY, password) VALUES ('user1', 'ch@ngem3a')")
        cursor.execute("UPDATE users SET gender = 'm', birth_year = '1980' WHERE KEY = 'user1'")
        cursor.execute("SELECT * FROM users WHERE KEY='user1'")
        res = cursor.fetchall()
        assert res == [[ 'user1', 1980, 'm', 'ch@ngem3a' ]], res

        cursor.execute("TRUNCATE users")

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("SELECT * FROM users WHERE KEY='user1'")
        res = cursor.fetchall()
        assert res == [], res


    @since('1.1')
    def undefined_column_handling_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            )
        """)

        cursor.execute("INSERT INTO test (k, v1, v2) VALUES (0, 0, 0)")
        cursor.execute("INSERT INTO test (k, v1) VALUES (1, 1)")
        cursor.execute("INSERT INTO test (k, v1, v2) VALUES (2, 2, 2)")

        cursor.execute("SELECT v2 FROM test")
        res = cursor.fetchall()
        assert res == [[0], [None], [2]], res

        cursor.execute("SELECT v2 FROM test WHERE k = 1")
        res = cursor.fetchall()
        assert res == [[None]], res

    @since('1.2')
    def range_tombstones_test(self):
        """ Test deletion by 'composite prefix' (range tombstones) """
        cluster = self.cluster

        # Uses 3 nodes just to make sure RowMutation are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        rows = 10
        col1 = 2
        col2 = 2
        cpr = col1 * col2
        for i in xrange(0, rows):
            for j in xrange(0, col1):
                for k in xrange(0, col2):
                    n = (i * cpr) + (j * col2) + k
                    cursor.execute("INSERT INTO test1 (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)" % (i, j, k, n, n))

        for i in xrange(0, rows):
            cursor.execute("SELECT v1, v2 FROM test1 where k = %d" % i)
            res = cursor.fetchall()
            assert res == [[x, x] for x in xrange(i * cpr, (i + 1) * cpr)], res

        for i in xrange(0, rows):
            cursor.execute("DELETE FROM test1 WHERE k = %d AND c1 = 0" % i)

        for i in xrange(0, rows):
            cursor.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            res = cursor.fetchall()
            assert res == [[x, x] for x in xrange(i * cpr + col1, (i + 1) * cpr)], res

        cluster.flush()
        time.sleep(0.2)

        for i in xrange(0, rows):
            cursor.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            res = cursor.fetchall()
            assert res == [[x, x] for x in xrange(i * cpr + col1, (i + 1) * cpr)], res

    @since('1.2')
    def range_tombstones_compaction_test(self):
        """ Test deletion by 'composite prefix' (range tombstones) with compaction """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 text,
                PRIMARY KEY (k, c1, c2)
            );
        """)


        for c1 in range(0, 4):
            for c2 in range(0, 2):
                cursor.execute("INSERT INTO test1 (k, c1, c2, v1) VALUES (0, %d, %d, %s)" % (c1, c2, '%i%i' % (c1, c2)))

        self.cluster.flush()

        cursor.execute("DELETE FROM test1 WHERE k = 0 AND c1 = 1")

        self.cluster.flush()
        self.cluster.compact()

        cursor.execute("SELECT v1 FROM test1 WHERE k = 0")
        res = cursor.fetchall()
        assert res == [ ['%i%i' % (c1, c2)] for c1 in xrange(0, 4) for c2 in xrange(0, 2) if c1 != 1], res

    @since('1.2')
    def delete_row_test(self):
        """ Test deletion of rows """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                 k int,
                 c1 int,
                 c2 int,
                 v1 int,
                 v2 int,
                 PRIMARY KEY (k, c1, c2)
            );
        """)

        q = "INSERT INTO test (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)"
        cursor.execute(q % (0, 0, 0, 0, 0))
        cursor.execute(q % (0, 0, 1, 1, 1))
        cursor.execute(q % (0, 0, 2, 2, 2))
        cursor.execute(q % (0, 1, 0, 3, 3))

        cursor.execute("DELETE FROM test WHERE k = 0 AND c1 = 0 AND c2 = 0")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert len(res) == 3, res

    @require('#3680')
    def range_query_2ndary_test(self):
        """ Test range queries with 2ndary indexes (#4257) """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE indextest (id int primary key, row int, setid int);")
        cursor.execute("CREATE INDEX indextest_setid_idx ON indextest (setid)")

        q =  "INSERT INTO indextest (id, row, setid) VALUES (%d, %d, %d);"
        cursor.execute(q % (0, 0, 0))
        cursor.execute(q % (1, 1, 0))
        cursor.execute(q % (2, 2, 0))
        cursor.execute(q % (3, 3, 0))

        cursor.execute("SELECT * FROM indextest WHERE setid = 0 AND row < 1;")
        res = cursor.fetchall()
        assert res == [[0, 0, 0]], res

    @since('1.1')
    def compression_option_validation_test(self):
        """ Check for unknown compression parameters options (#4266) """
        cursor = self.prepare()

        assert_invalid(cursor, """
          CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
          WITH compression_parameters:sstable_compressor = 'DeflateCompressor';
        """)

    @since('1.1')
    def keyspace_creation_options_test(self):
        """ Check one can use arbitrary name for datacenter when creating keyspace (#4278) """
        cursor = self.prepare()

        # we just want to make sure the following is valid
        cursor.execute("""
            CREATE KEYSPACE Foo
                WITH strategy_class = NetworkTopologyStrategy
                AND strategy_options:"us-east"=1
                AND strategy_options:"us-west"=1;
        """)

    @since('1.2')
    def set_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags set<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        cursor.execute(q % "tags = tags + { 'foo' }")
        cursor.execute(q % "tags = tags + { 'bar' }")
        cursor.execute(q % "tags = tags + { 'foo' }")
        cursor.execute(q % "tags = tags + { 'foobar' }")
        cursor.execute(q % "tags = tags - { 'bar' }")

        cursor.execute("SELECT tags FROM user")
        assert_json(cursor.fetchall(), ['foo', 'foobar'])

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        cursor.execute(q % "tags = { 'a', 'c', 'b' }")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['a', 'b', 'c'])

        time.sleep(.01)

        cursor.execute(q % "tags = { 'm', 'n' }")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['m', 'n'])

        cursor.execute("DELETE tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[None]], re


    @since('1.2')
    def map_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                m map<text, int>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        cursor.execute(q % "m['foo'] = 3")
        cursor.execute(q % "m['bar'] = 4")
        cursor.execute(q % "m['woot'] = 5")
        cursor.execute(q % "m['bar'] = 6")
        cursor.execute("DELETE m['foo'] FROM user WHERE fn='Tom' AND ln='Bombadil'")

        cursor.execute("SELECT m FROM user")
        assert_json(cursor.fetchall(), { 'woot': 5, 'bar' : 6 })

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        cursor.execute(q % "m = { 'a' : 4 , 'c' : 3, 'b' : 2 }")
        cursor.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), {'a' : 4, 'b' : 2, 'c' : 3 })

        time.sleep(.01)

        # Check we correctly overwrite
        cursor.execute(q % "m = { 'm' : 4 , 'n' : 1, 'o' : 2 }")
        cursor.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), {'m' : 4, 'n' : 1, 'o' : 2 })

    @since('1.2')
    def list_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags list<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        cursor.execute(q % "tags = tags + [ 'foo' ]")
        cursor.execute(q % "tags = tags + [ 'bar' ]")
        cursor.execute(q % "tags = tags + [ 'foo' ]")
        cursor.execute(q % "tags = tags + [ 'foobar' ]")


        cursor.execute("SELECT tags FROM user")
        assert_json(cursor.fetchall(), ['foo', 'bar', 'foo', 'foobar'])

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        cursor.execute(q % "tags = [ 'a', 'c', 'b', 'c' ]")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['a', 'c', 'b', 'c'])

        cursor.execute(q % "tags = [ 'm', 'n' ] + tags")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['n', 'm', 'a', 'c', 'b', 'c'])

        cursor.execute(q % "tags[2] = 'foo', tags[4] = 'bar'")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['n', 'm', 'foo', 'c', 'bar', 'c'])

        cursor.execute("DELETE tags[2] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['n', 'm', 'c', 'bar', 'c'])

        cursor.execute(q % "tags = tags - [ 'bar' ]")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert_json(cursor.fetchall(), ['n', 'm', 'c', 'c'])

    @since('1.2')
    def multi_collection_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE foo(
                k uuid PRIMARY KEY,
                L list<int>,
                M map<text, int>,
                S set<int>
            );
        """)

        cursor.execute("UPDATE ks.foo SET L = [1, 3, 5] WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';")
        cursor.execute("UPDATE ks.foo SET L = L + [7, 11, 13] WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';")
        cursor.execute("UPDATE ks.foo SET S = {1, 3, 5} WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';")
        cursor.execute("UPDATE ks.foo SET S = S + {7, 11, 13} WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';")
        cursor.execute("UPDATE ks.foo SET M = {'foo': 1, 'bar' : 3} WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';")
        cursor.execute("UPDATE ks.foo SET M = M + {'foobar' : 4} WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';")

        cursor.execute("SELECT L, M, S FROM foo WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008'")
        res = cursor.fetchall()
        assert_json(res, [1, 3, 5, 7, 11, 13], col=0)
        assert_json(res, {'foo' : 1, 'bar' : 3, 'foobar' : 4}, col=1)
        assert_json(res, [1, 3, 5, 7, 11, 13], col=2)

    @since('1.1')
    def range_query_test(self):
        """ Range test query from #4372 """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )")

        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5');")

        cursor.execute("SELECT a, b, c, d, e, f FROM test WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2;")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1, 2, u'2'], [1, 1, 1, 1, 3, u'3'], [1, 1, 1, 1, 5, u'5']], res

    @since('1.1.2')
    def update_type_test(self):
        """ Test altering the type of a column, including the one in the primary key (#4041) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k text,
                c text,
                v text,
                PRIMARY KEY (k, c)
            )
        """)

        req = "INSERT INTO test (k, c, v) VALUES ('%s', '%s', '%s')"
        # using utf8 character so that we can see the transition to BytesType
        cursor.execute(req % ('ɸ', 'ɸ', 'ɸ'))

        cursor.execute("SELECT * FROM test")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[u'ɸ', u'ɸ', u'ɸ']], res

        cursor.execute("ALTER TABLE test ALTER v TYPE blob")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        # the last should not be utf8 but a raw string
        assert res == [[u'ɸ', u'ɸ', 'ɸ']], res

        cursor.execute("ALTER TABLE test ALTER k TYPE blob")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [['ɸ', u'ɸ', 'ɸ']], res

        cursor.execute("ALTER TABLE test ALTER c TYPE blob")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [['ɸ', 'ɸ', 'ɸ']], res

    @require('#4179')
    def composite_row_key_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                c int,
                v int,
                PRIMARY KEY ((k1, k2), c)
            )
        """)

        req = "INSERT INTO test (k1, k2, c, v) VALUES (%d, %d, %d, %d)"
        for i in range(0, 4):
            cursor.execute(req % (0, i, i, i))

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], res

        cursor.execute("SELECT * FROM test WHERE k1 = 0 and k2 IN (1, 3)")
        res = cursor.fetchall()
        assert res == [[0, 1, 1, 1], [0, 3, 3, 3]], res

        assert_invalid(cursor, "SELECT * FROM test WHERE k2 = 3")
        assert_invalid(cursor, "SELECT * FROM test WHERE k1 IN (0, 1) and k2 = 3")

        cursor.execute("SELECT * FROM test WHERE token(k1, k2) = token(0, 1)")
        res = cursor.fetchall()
        assert res == [[0, 1, 1, 1]], res

        cursor.execute("SELECT * FROM test WHERE token(k1, k2) > '0'")
        res = cursor.fetchall()
        assert res == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], res

    @require('#4377')
    def cql3_insert_thrift_test(self):
        """ Check that we can insert from thrift into a CQL3 table (#4377) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        cli = self.cluster.nodelist()[0].cli()
        cli.do("use ks")
        cli.do("set test[2]['4:v'] = int(200)")
        assert not cli.has_errors(), cli.errors()
        assert False, cli.last_output()

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [ "foo" ], res

    @since('1.2')
    def row_existence_test(self):
        """ Check the semantic of CQL row existence (part of #4361) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c)
            )
        """)

        cursor.execute("INSERT INTO test (k, c, v1, v2) VALUES (1, 1, 1, 1)")

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1]], res

        assert_invalid(cursor, "DELETE c FROM test WHERE k = 1 AND c = 1")

        cursor.execute("DELETE v2 FROM test WHERE k = 1 AND c = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, None]], res

        cursor.execute("DELETE v1 FROM test WHERE k = 1 AND c = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[1, 1, None, None]], res

        cursor.execute("DELETE FROM test WHERE k = 1 AND c = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("INSERT INTO test (k, c) VALUES (2, 2)")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[2, 2, None, None]], res

    @since('1.2')
    def only_pk_test(self):
        """ Check table with only a PK (#4361) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                PRIMARY KEY (k, c)
            )
        """)

        q = "INSERT INTO test (k, c) VALUES (%d, %d)"
        for k in range(0, 2):
            for c in range(0, 2):
                cursor.execute(q % (k, c))

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[x, y] for x in range(0, 2) for y in range(0, 2)], res

        # Check for dense tables too
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
        """)

        q = "INSERT INTO test2 (k, c) VALUES (%d, %d)"
        for k in range(0, 2):
            for c in range(0, 2):
                cursor.execute(q % (k, c))

        cursor.execute("SELECT * FROM test2")
        res = cursor.fetchall()
        assert res == [[x, y] for x in range(0, 2) for y in range(0, 2)], res

    @since('1.2')
    def date_test(self):
        """ Check dates are correctly recognized and validated """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timestamp
            )
        """)

        cursor.execute("INSERT INTO test (k, t) VALUES (0, '2011-02-03')")
        assert_invalid(cursor, "INSERT INTO test (k, t) VALUES (0, '2011-42-42')")

    def range_slice_test(self):
        """ Test a regression from #1337 """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)

        cursor.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)

        cursor.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        cursor.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert len(res) == 2, res

