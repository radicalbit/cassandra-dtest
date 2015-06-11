from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since

class TestMultipleDisks(Tester):

    def verify_keys_test(self):
        """
        Test to see that all keys are on the same disk:
        Start up 3 node cluster, with 2 data disks.
        Write a number of partitions to cluster, remembering keys.
        Run several queries with tracing enabled.
        Check that all keys are on same disk.
        """
        pass

    def fill_disk_test(self):
        """
        Test to see that proper behaviour occurs when a disk is full:
        Start up 3 node cluster, with 2 data disks (of preferably limited storage).
        Note disk_failure_policy should be set to best-effort.
        Write data to cluster as background task, inserting new keys until disk is full.
        Once disk is full, ensure that new writes go to disk 2.
        """
        pass

    def corrupt_disk_test(self):
        """
        Test to see that corrupted/bad disks are handled properly:
        Start up 3 node cluster, with 2 data disks.
        Note disk_failure_policy should be set to best-effort.
        Write data to cluster as background task.
        While doing so intentionally corrupt a disk.
        Once disk is corrupted, ensure that new writes go to disk 2.
        """
        pass

    def add_disk_test(self):
        """
        Test adding a disk works properly:
        Start up 3 node cluster, with 2 data disks, but only first specified in yaml.
        Note disk_failure_policy should be set to best-effort.
        Write decent amount of data then stop cluster.
        Add additional disk to yaml and start cluster.
        Ensure that data files properly distributed across disks.      
        """
        pass

    def vnode_compaction_test(self):
        """
        Test VnodeAwareCompactionStrategy.
        Start up 3 node cluster with 2 data disks.
        Write data and flush.
        Run compaction.
        Ensure data on disks is still present and that compaction was successful.
        """
        pass