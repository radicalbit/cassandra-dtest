from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since
import subprocess
import os
import time

class TestJBOD(Tester):

    def verify_keys_test(self):
        """
        Test to see that all keys are on the same disk:
        Start up 3 node cluster, with 2 data disks.
        Write a number of partitions to cluster, remembering keys.
        Use getsstables to see which sstables each key is on. 
        Check that all keys are on same disk.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        cluster.set_configuration_options({'disk_failure_policy':'best_effort'})

        cluster.set_data_dirs(['data1', 'data2'])
        cluster.start()

        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE test.jtest(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        #insert a bunch of keys
        numkeys=100
        for x in range(0, numkeys):
            insq = "INSERT INTO test.jtest(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)
        node1.flush()

        initial_sstables = []
        #figure out which dir each key is in
        for x in range(0, numkeys):
            sstable_path = node1.nodetool('getsstables test jtest ' + str(x), capture_output=True)
            if "data1" in path:
                sstables[x] = "data1"
            else:
                sstables[x] = "data2"

        #update added keys to ensure that keys remain on the same disk
        for x in range(0, numkeys):
            insq = "INSERT INTO test.jtest(key,val) VALUES ({key}, 66)".format(key=str(x))
            cursor.execute(insq)
        node1.flush()

        #check that keys are on same disks as prior
        final_sstables = []
        for x in range(0, numkeys):
            sstable_paths = node1.nodetool('getsstables test jtest ' + str(x), capture_output=True)
            for path in sstable_paths:
                self.assertTrue(sstables[x] in path)
    
    def add_disk_test(self):
        """
        Test adding a disk works properly:
        Start up 3 node cluster, with 2 data disks, but only first specified in yaml.
        Note disk_failure_policy should be set to best-effort.
        Write decent amount of data then stop cluster.
        Add additional disk to yaml and remove one existing disk and start cluster.
        Ensure that data files properly distributed across disks.      
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        cluster.set_data_dirs(['data1', 'data2'])
        cluster.set_configuration_options({'disk_failure_policy':'best_effort'})

        cluster.start()

        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE test.jtest(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)
        
        numkeys=100
        for x in range(0, numkeys):
            insq = "INSERT INTO test.jtest(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)
        node1.flush()

        cluster.stop(gently=False)
        cluster.set_data_dirs(['data2', 'data3', 'data4'])
        cluster.start()
        
        numkeys=100
        for x in range(0, numkeys):
            insq = "SELECT key FROM test.jtest(key,val) WHERE key={key};".format(key=str(x))
            try:
                res = cursor.execute(insq)
                self.assertEqual(res[0][0])
            except Exception, e:
                self.fail("Failed during key verification: " + str(e))

    def vnode_compaction_test(self):
        """
        Test VnodeAwareCompactionStrategy.
        Start up 3 node cluster with 2 data disks.
        Write data and flush.
        Run compaction.
        Ensure data on disks is still present and that compaction was successful.
        """
        pass