from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since, InterruptBootstrap
from time import time
from assertions import assert_none, assert_invalid

class TestHintedHandoff(Tester):

    def check_delivery(self, node):
        mbean = make_mbean('db', 'HintedHandoffManager')
        handedoff = False
        timeout = time() + 70.00
        while handedoff == False and time() < timeout:
            with JolokiaAgent(node) as jmx:
                pending_nodes = jmx.execute_method(mbean, 'listEndpointsPendingHints')
                if len(pending_nodes) == 0:
                    handedoff = True
        return handedoff, pending_nodes


    def simple_functionality_test(self):
        cluster = self.cluster
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)
        node3.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab(key,val) WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node2.start()
        node3.start()

        handedoff, pending_nodes = self.check_delivery(node1)

        self.assertTrue(handedoff, msg=pending_nodes)

        node1.stop(gently=False)

        for node in [node2,node3]:
            othernode = [x for x in [node2, node3] if not node == x][0]
            othernode.stop(gently)
            cursor = self.patient_cql_connection(node)
            for x in range(200, numhints):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], x)
            for x in range(100, 200):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], 100000)
            for x in range(0, 100):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                assert_none(cursor, query)
            othernode.start(wait=True)


    def upgrade_versions_test(self):
        cluster = self.cluster
        cluster.set_install_dir(version="2.2.0-beta1")
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)
        node3.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab(key,val) WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node1.stop(gently=False)
        node1.set_install_dir(version = '3.0')
        node1.start()

        #check system.hints truncated/flat file written (TODO)
        assert_invalid(cursor, "SELECT * FROM system.hints LIMIT 10")

        node2.set_install_dir(version = '3.0')
        node3.set_install_dir(version = '3.0')

        node2.start()
        node3.start()

        handedoff, pending_nodes = self.check_delivery(node1)

        self.assertTrue(handedoff, msg=pending_nodes)

        node1.stop(gently=False)

        for node in [node2,node3]:
            othernode = [x for x in [node2, node3] if not node == x][0]
            othernode.stop(gently)
            cursor = self.patient_cql_connection(node)
            for x in range(200, numhints):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], x)
            for x in range(100, 200):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], 100000)
            for x in range(0, 100):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                assert_none(cursor, query)
            othernode.start(wait=True)

    def nodetool_commands_test(self):
        cluster = self.cluster
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node1.nodetool("sethintedhandoffthrottlekb 10")

        node3.stop(gently=False)

        numhints = 10000        
        for x in range(0, numhints):
            insq = SimpleStatement("INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x)), consistency_level=ConsistencyLevel.TWO)
            cursor.execute(insq)
        
        node3.start()

        notstarted = True
        while notstarted:
            output = node1.nodetool("statushandoff", capture_output=True)
            if "running" in output:
                notstarted = False

        node1.nodetool("pausehandoff")

        output = node1.nodetool("statushandoff", capture_output=True)
        self.assertTrue("paused" in output)
        
        node1.nodetool("resumehandoff")

        output = node1.nodetool("statushandoff", capture_output=True)
        self.assertTrue("running" in output)

        handedoff, pending_nodes = self.check_delivery(node1)
        self.assertTrue(handedoff, msg=pending_nodes)

    def interrupted_bootstrap_test(self):
        """Test interrupting bootstrap on hint-destined node"""

        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()

        #create ks and table with rf 2
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':2}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        #stop node2 to allow hints to accumulate
        node2.stop(wait_other_notice=True, gently=False)

        #write data to generate hints
        numhints=100
        for x in range(0,numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        #check hints table to ensure hints are properly generated
        hintcount = cursor.execute("select count(*) from system.hints;")
        self.assertTrue(hintcount[0][0] == numhints)

        # start node2 but kill in the process of bootstrapping to see if can cause Missing Host Id error
        failline = "Starting up server gossip"
        t = InterruptBootstrap(node2, message=failline)
        t.start()
        try:
            node2.start()
        except Exception, e:
            debug(str(e))
        t.join()

        time.sleep(5)

        #start properly to deliver hints
        try:
            node2.start(wait_other_notice=True)
        except Exception, e:
            debug(e)

        try:
            node2.watch_log_for_alive(node1, node2.mark_log(), timeout=10)
        except Exception, e:
            debug(e)

        #check hints delivered
        hintcount = cursor.execute("select count(*) from system.hints;")
        self.assertTrue(hintcount[0][0]==0)

        #verify hints delivered
        for x in range(0, numhints):
            query = SimpleStatement("SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x)), consistency_level=ConsistencyLevel.TWO)
            results = cursor.execute(query)
            self.assertEqual(results[0][0], x)
