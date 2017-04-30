"""
MongoDB Support

"""
import datetime
from pymongo import errors
import logging
import pymongo
import re

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class MongoDB(base.Plugin):
    # SETUP: Set this GUID to something unique.
    GUID = 'CHANGE.ME'

    def add_datapoints(self, name, stats):
        """Add all of the data points for a database

        :param str name: The name of the database for the stats
        :param dict stats: The stats data to add

        """
        # SETUP: You can add some database name manipulation here to rework bad named DBs
        base_key = name

        self.add_gauge_value('Database/Extents/{0}'.format(base_key), 'extents', stats.get('numExtents', 0))
        self.add_gauge_value('Database/Size/{0}'.format(base_key), 'bytes', stats.get('dataSize', 0))
        self.add_gauge_value('Database/FileSize/{0}'.format(base_key), 'bytes', stats.get('storageSize', 0))
        self.add_gauge_value('Database/ObjectsCount/{0}'.format(base_key), 'objects', stats.get('objects', 0))
        self.add_gauge_value('Database/ObjectsAverageSize/{0}'.format(base_key), 'bytes', stats.get('avgObjSize', 0))
        self.add_gauge_value('Database/Collections/{0}'.format(base_key), 'collections', stats.get('collections', 0))
        self.add_gauge_value('Database/IndexCount/{0}'.format(base_key), 'indexes', stats.get('indexes', 0))
        self.add_gauge_value('Database/IndexSize/{0}'.format(base_key), 'bytes', stats.get('indexSize', 0))

    def add_server_datapoints(self, stats):
        """Add all of the data points for a server

        :param dict stats: The stats data to add

        """
        host = stats.get('host','no_host')

        asserts = stats.get('asserts', dict())
        self.add_derive_value('Asserts/Regular/{0}'.format(host), 'asserts', asserts.get('regular', 0))
        self.add_derive_value('Asserts/Warning/{0}'.format(host), 'asserts', asserts.get('warning', 0))
        self.add_derive_value('Asserts/Message/{0}'.format(host), 'asserts', asserts.get('msg', 0))
        self.add_derive_value('Asserts/User/{0}'.format(host), 'asserts', asserts.get('user', 0))
        self.add_derive_value('Asserts/Rollovers/{0}'.format(host), 'asserts', asserts.get('rollovers', 0))

        flush = stats.get('backgroundFlushing', dict())
        self.add_derive_timing_value('BackgroundFlushes/{0}'.format(host), 'ms', flush.get('flushes', 0), flush.get('total_ms', 0), flush.get('last_ms', 0))
        self.add_gauge_value('SecondsSinceLastFlush/{0}'.format(host), 'seconds', (datetime.datetime.now() - flush.get('last_finished', datetime.datetime.now())).seconds)

        conn = stats.get('connections', dict())
        self.add_gauge_value('Connections/Available/{0}'.format(host), 'connections', conn.get('available', 0))
        self.add_gauge_value('Connections/Current/{0}'.format(host), 'connections', conn.get('current', 0))

        metrics = stats.get('metrics', dict())

        commands = metrics.get('commands', dict())
        self.add_derive_value('Metrics/Commands/Find/{0}'.format(host), 'ops', commands.get('find',dict()).get('total',0))
        self.add_derive_value('Metrics/Commands/Count/{0}'.format(host), 'ops', commands.get('count',dict()).get('total',0))
        self.add_derive_value('Metrics/Commands/CreateIndexes/{0}'.format(host), 'ops', commands.get('createIndexes',dict()).get('total',0))
        self.add_derive_value('Metrics/Commands/MoveChunks/{0}'.format(host), 'ops', commands.get('moveChunk',dict()).get('total',0))
        self.add_derive_value('Metrics/Commands/Update/{0}'.format(host), 'ops', commands.get('update',dict()).get('total',0))
        self.add_derive_value('Metrics/Commands/Distinct/{0}'.format(host), 'ops', commands.get('distinct',dict()).get('total',0))
        self.add_derive_value('Metrics/Commands/GetMore/{0}'.format(host), 'ops', commands.get('getMore',dict()).get('total',0))

        document = metrics.get('document', dict())
        self.add_derive_value('Metrics/Documents/Deleted/{0}'.format(host), 'ops', document.get('deleted', 0))
        self.add_derive_value('Metrics/Documents/Returned/{0}'.format(host), 'ops', document.get('returned', 0))
        self.add_derive_value('Metrics/Documents/Inserted/{0}'.format(host), 'ops', document.get('inserted', 0))
        self.add_derive_value('Metrics/Documents/Updated/{0}'.format(host), 'ops', document.get('updated', 0))

        operations = metrics.get('operation', dict())
        self.add_derive_value('Metrics/NonIndex/Ordering/{0}'.format(host), 'ops', operations.get('scanAndOrder', 0))

        repl = metrics.get('repl', dict())
        repl_network = repl.get('network', dict())
        repl_network_get_mores = repl_network.get('getmores')
        self.add_derive_value('Metrics/Repl/GetMores/count/{0}'.format(host), 'ops', repl_network_get_mores.get('num', 0))
        self.add_derive_value('Metrics/Repl/GetMores/total/{0}'.format(host), 'ops', repl_network_get_mores.get('totalMillis', 0))
        # metrics.repl.network.getmores.num GetMore that cross shards

        cursors = metrics.get('cursors', dict())
        self.add_gauge_value('Cursors/On/{0}'.format(host), 'cursors', cursors.get('open', 0))
        self.add_derive_value('Cursors/TimedOut/{0}'.format(host), 'cursors', cursors.get('timedOut', 0))

        dur = stats.get('dur', dict())
        self.add_gauge_value('Durability/CommitsInWriteLock/{0}'.format(host), 'commits', dur.get('commitsInWriteLock', 0))
        self.add_gauge_value('Durability/EarlyCommits/{0}'.format(host), 'commits', dur.get('earlyCommits', 0))
        self.add_gauge_value('Durability/JournalCommits/{0}'.format(host), 'commits', dur.get('commits', 0))
        self.add_gauge_value('Durability/JournalBytesWritten/{0}'.format(host), 'bytes', dur.get('journaledMB', 0) / 1048576)
        self.add_gauge_value('Durability/DataFileBytesWritten/{0}'.format(host), 'bytes', dur.get('writeToDataFilesMB', 0) / 1048576)

        timems = dur.get('timeMs', dict())
        self.add_gauge_value('Durability/Timings/DurationMeasured/{0}'.format(host), 'ms', timems.get('dt', 0))
        self.add_gauge_value('Durability/Timings/LogBufferPreparation/{0}'.format(host), 'ms', timems.get('prepLogBuffer', 0))
        self.add_gauge_value('Durability/Timings/WriteToJournal/{0}'.format(host), 'ms', timems.get('writeToJournal', 0))
        self.add_gauge_value('Durability/Timings/WriteToDataFiles/{0}'.format(host), 'ms', timems.get('writeToDataFiles', 0))
        self.add_gauge_value('Durability/Timings/RemapingPrivateView/{0}'.format(host), 'ms', timems.get('remapPrivateView', 0))

        locks = stats.get('globalLock', dict())
        self.add_derive_value('GlobalLocks/Held/{0}'.format(host), 'ms', locks.get('lockTime', 0) / 1000)
        self.add_derive_value('GlobalLocks/Ratio/{0}'.format(host), 'ratio', locks.get('ratio', 0))

        active = locks.get('activeClients', dict())
        self.add_derive_value('GlobalLocks/ActiveClients/Total/{0}'.format(host), 'clients', active.get('total', 0))
        self.add_derive_value('GlobalLocks/ActiveClients/Readers/{0}'.format(host), 'clients', active.get('readers', 0))
        self.add_derive_value('GlobalLocks/ActiveClients/Writers/{0}'.format(host), 'clients', active.get('writers', 0))

        queue = locks.get('currentQueue', dict())
        self.add_derive_value('GlobalLocks/Queue/Total/{0}'.format(host), 'locks', queue.get('total', 0))
        self.add_derive_value('GlobalLocks/Queue/Readers/{0}'.format(host), 'readers', queue.get('readers', 0))
        self.add_derive_value('GlobalLocks/Queue/Writers/{0}'.format(host), 'writers', queue.get('writers', 0))

        mem = stats.get('mem', dict())
        self.add_gauge_value('Memory/Resident/{0}'.format(host), 'megabytes', mem.get('resident', 0))
        self.add_gauge_value('Memory/Virtual/{0}'.format(host), 'megabytes', mem.get('virtual', 0))

        net = stats.get('network', dict())
        self.add_derive_value('Network/Requests/{0}'.format(host), 'requests', net.get('numRequests', 0))
        self.add_derive_value('Network/Transfer/In/{0}'.format(host), 'bytes', net.get('bytesIn', 0))
        self.add_derive_value('Network/Transfer/Out/{0}'.format(host), 'bytes', net.get('bytesOut', 0))

        ops = stats.get('opcounters', dict())
        self.add_derive_value('Operations/Insert/{0}'.format(host), 'ops', ops.get('insert', 0))
        self.add_derive_value('Operations/Query/{0}'.format(host), 'ops', ops.get('query', 0))
        self.add_derive_value('Operations/Update/{0}'.format(host), 'ops', ops.get('update', 0))
        self.add_derive_value('Operations/Delete/{0}'.format(host), 'ops', ops.get('delete', 0))
        self.add_derive_value('Operations/GetMore/{0}'.format(host), 'ops', ops.get('getmore', 0))
        self.add_derive_value('Operations/Command/{0}'.format(host), 'ops', ops.get('command', 0))

        extra = stats.get('extra_info', dict())
        self.add_gauge_value('System/HeapUsage/{0}'.format(host), 'bytes', extra.get('heap_usage_bytes', 0))
        self.add_derive_value('System/PageFaults/{0}'.format(host), 'faults', extra.get('page_faults', 0))

        wt = stats.get('wiredTiger', dict())
        wt_cache = wt.get('cache', dict())
        self.add_derive_value('WiredTiger/Cache/BytesReadInto/Derived/{0}'.format(host), 'bytes', wt_cache.get('bytes read into cache', 0))
        self.add_derive_value('WiredTiger/Cache/BytesIn/Derived/{0}'.format(host), 'bytes', wt_cache.get('bytes currently in the cache', 0))

        self.add_gauge_value('WiredTiger/Cache/DirtyBytes/{0}'.format(host), 'bytes', wt_cache.get('tracked dirty bytes in the cache', 0))
        self.add_gauge_value('WiredTiger/Cache/BytesReadInto/Gauge/{0}'.format(host), 'bytes', wt_cache.get('bytes read into cache', 0))
        self.add_gauge_value('WiredTiger/Cache/BytesIn/Gauge/{0}'.format(host), 'bytes', wt_cache.get('bytes currently in the cache', 0))

        wt_concurrent = wt.get('concurrentTransactions', dict())
        wt_write = wt_concurrent.get('write', dict())
        self.add_gauge_value('WiredTiger/concurrentTransactions/WritesAvailable/{0}'.format(host), 'tickets', wt_write.get('available', 0))

        wt_read = wt_concurrent.get('read', dict())
        self.add_gauge_value('WiredTiger/concurrentTransactions/ReadsAvailable/{0}'.format(host), 'tickets', wt_read.get('available', 0))

        top_repl = stats.get('repl', dict())
        if top_repl.get('ismaster', False):
            repl_buffer = repl.get('buffer', dict())
            self.add_derive_value('Repl/Buffer/Count/{0}'.format(host), 'ops', repl_buffer.get('count', 0))
            self.add_derive_value('Repl/Buffer/SizeBytes/{0}'.format(host), 'bytes', repl_buffer.get('sizeBytes', 0))
            self.add_derive_value('Repl/Buffer/MaxSizeBytes/{0}'.format(host), 'bytes', repl_buffer.get('maxSizeBytes', 0))
        else:
            repl_apply = repl.get('apply', dict())
            repl_apply_batches = repl_apply.get('batches', dict())
            self.add_derive_value('Repl/Apply/Ops/{0}'.format(host), 'ops', repl_apply.get('ops', 0))
            self.add_derive_value('Repl/Apply/BatchesNum/{0}'.format(host), 'bytes', repl_apply_batches.get('num', 0))
            self.add_derive_value('Repl/Apply/BatchesTotalMillis/{0}'.format(host), 'bytes', repl_apply_batches.get('totalMillis', 0))

    def connect(self):
        kwargs = {'host': self.config.get('host', 'localhost'), 'port': self.config.get('port', 27017)}
        for key in ['ssl', 'ssl_keyfile', 'ssl_certfile', 'ssl_cert_reqs', 'ssl_ca_certs']:
            if key in self.config:
                kwargs[key] = self.config[key]
        try:
            client =  pymongo.MongoClient(**kwargs)
            username = self.config.get('admin_username', 'root')
            password = self.config.get('admin_password', 'password')
            auth_db = self.config.get('authDB', 'admin')
            client[auth_db].authenticate(username, password)
            return client
        except pymongo.errors.ConnectionFailure as error:
            LOGGER.error('Could not connect to MongoDB: %s', error)

    def get_and_add_db_dict(self, databases):
        """Handle the nested database structure with username and password.

        :param dict/list databases: The databases data structure

        """
        LOGGER.debug('Processing mongo databases')
        client = self.connect()
        if not client:
            return
        databases = self.config.get('databases', list())
        try:
            if (isinstance(databases, dict)):
                for database in databases.keys():
                    db = client[database]
                    logged_in = False
                    if 'username' in databases[database]:
                        db.authenticate(databases[database]['username'], databases[database].get('password'))
                        logged_in = True
                    self.add_datapoints(database, db.command('dbStats'))
                    if logged_in:
                        db.logout()
            else:
                for database in databases:
                    db = client[database]
                    self.add_datapoints(database, db.command('dbStats'))
        except errors.OperationFailure as error:
            LOGGER.critical('Could not fetch stats: %s', error)

    def get_and_add_server_stats(self):
        LOGGER.debug('Fetching server stats')
        client = self.connect()
        if not client:
            return
        self.add_server_datapoints(client.db.command('serverStatus'))
        client.close()

    def poll(self):
        self.initialize()
        self.get_and_add_server_stats()
        self.get_and_add_db_dict()
        self.finish()
