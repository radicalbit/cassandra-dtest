import time
from threading import Event

from dtest import Tester, debug
from jmxutils import JolokiaAgent, make_mbean


class ThreadActionsComplete(Exception):
    """
    Exception to be used as a signal for when a thread has finished work.
    """


class FileDescriptorLeakTest(Tester):

    def test_lots_of_flushing(self):
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        self.flush_until_sstable_count(cluster, desired_sstable_count=4000, max_wait_s=1800)

    def flush_until_sstable_count(self, cluster, desired_sstable_count=1000, stress_command_list=None, max_wait_s=600):
        """
        Runs stress and flush until the specified number of flushes are done.
        This should result in the same number of sstables as flushes completed.
        Disables auto compaction in the given cluster. Reenable later if needed.
        Errors out if max_wait is exceeded.
        """
        if stress_command_list is None:
            stress_command_list = ['write', 'n=500K', '-rate', 'threads=100', 'limit=5000/s']

        node1 = cluster.nodelist()[0]

        try:
            agents = [JolokiaAgent(node) for node in cluster.nodelist()]
            mbean = make_mbean('db', 'StorageService')
            for agent in agents:
                agent.start()

            stress_starting = Event()
            stress_stopped = Event()
            flushes_done = Event()

            def flush_cluster(count):
                this_function = flush_cluster  # for readability below

                # use a counting attribute on this function itself
                if getattr(this_function, 'flushes_req_count', None) is None:
                    this_function.flushes_req_count = 0

                # give stress a chance to start writing data before we begin flushing aggressively
                if stress_starting.is_set():
                    debug("waiting for stress to finish starting")
                    time.sleep(20)
                    stress_starting.clear()  # assume stress has started by now
                    stress_stopped.clear()

                if stress_stopped.is_set():
                    if divmod(count, 50)[1] == 0:
                        debug("stress is stopping. waiting for it to start again.")
                    time.sleep(0.1)
                    return

                # we dont' want compaction merging our sstables together
                if count == 0:
                    cluster.nodetool("disableautocompaction keyspace1 standard1")

                if this_function.flushes_req_count < desired_sstable_count:
                    for i, agent in enumerate(agents):
                        agent.execute_method(mbean, 'forceKeyspaceFlush', arguments=['keyspace1', 'standard1'])
                    this_function.flushes_req_count += 1

                    if divmod(this_function.flushes_req_count, 50)[1] == 0:
                        debug("Flush thread alive, current execution count: {}".format(this_function.flushes_req_count))
                else:
                    debug("Flush called enough. Stopping thread.")
                    flushes_done.set()
                    raise ThreadActionsComplete()

            def stress_cluster(count):
                stress_starting.set()
                node1.stress(stress_command_list)
                stress_starting.clear()
                stress_stopped.set()

                if flushes_done.is_set():
                    debug("flushes are done, stress loop exiting.")
                    raise ThreadActionsComplete()

            def check_runner_for_errors(runner, label=''):
                try:
                    if runner.check() is None:
                        debug("{} -- thread is ok".format(label))
                except ThreadActionsComplete, e:
                    # this exception already stopped the thread in question
                    # and the proper event should have been set
                    # so there's nothing to do about it here except ignore
                    pass
                except Exception, e:
                    debug(e)
                    raise RuntimeError("{} -- error occured in thread".format(label))

            stress_runner = self.go(stress_cluster)
            flush_runner = self.go(flush_cluster)

            expiry = time.time() + max_wait_s
            while time.time() < expiry:
                check_runner_for_errors(flush_runner, 'check, flush thread')
                check_runner_for_errors(stress_runner, 'check, stress thread')
                time.sleep(15)  # sleep's fine here, everything is happening in background threads anyway

                # our signal that enough flushes have run
                if flushes_done.is_set():
                    break
            else:
                raise RuntimeError("Ran out of time waiting for sstable creation.")

            for line in node1.nodetool('cfstats keyspace1.standard1', True):
                print(line)
        finally:
            for agent in agents:
                # best effort at stopping jolokia, but let's not allow it to fail a test if stop doesn't work
                try:
                    agent.stop()
                except Exception:
                    pass
