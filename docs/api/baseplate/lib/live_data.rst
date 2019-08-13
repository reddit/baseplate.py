``baseplate.lib.live_data``
===========================

This component of Baseplate provides real-time synchronization of data across a
cluster of servers. It is intended for situations where data is read
frequently, does not change super often, and when it does change needs to
change everywhere at once. In most cases, this will be an underlying feature of
some other system (e.g. an experiments framework.)

There are four main components of the live data system:

* `ZooKeeper`_, a highly available data store that can push change notifications.
* The watcher, a sidecar daemon that watches nodes in ZooKeeper and syncs their
  contents to disk.
* :py:class:`~baseplate.lib.file_watcher.FileWatcher` instances in your
  application that load the synchronized data into memory.
* Something that writes to ZooKeeper (potentially the writer tool).

The watcher daemon and tools for writing data to ZooKeeper are covered on this
page.

.. _ZooKeeper: https://zookeeper.apache.org/

Watcher Daemon
--------------

The watcher daemon is a sidecar that watches nodes in ZooKeeper and syncs their
contents to local files on change. It is entirely configured via INI file and
is run like so:

.. code-block:: console

   $ python -m baseplate.sidecars.live_data_watcher some_config.ini

Where ``some_config.ini`` might look like:

.. code-block:: ini

   [live-data]
   zookeeper.hosts = zk01:2181,zk02:2181
   zookeeper.credentials = secret/myservice/zookeeper_credentials

   nodes.a.source = /a/node/in/zookeeper
   nodes.a.dest = /var/local/file-on-disk

   nodes.b.source = /another/node/in/zookeeper
   nodes.b.dest = /var/local/another-file
   nodes.b.owner = www-data
   nodes.b.group = www-data
   nodes.b.mode = 0400

Each of the defined ``nodes`` will be watched by the daemon.

The watcher daemon will touch the ``mtime`` of the local files periodically to
indicative liveliness to monitoring tools.


The Writer Tool
---------------

For simple cases where you just want to put the contents of a file into
ZooKeeper (perhaps in a CI task) you can use the live data writer. It expects a
configuration file with ZooKeeper connection information, like the watcher, and
takes some additional parameters on the command line.

.. code-block:: console

   $ python -m baseplate.lib.live_data.writer some_config.ini \
      input.json /some/node/in/zookeeper
   Writing input.json to ZooKeeper /some/node/in/zookeeper...
   ---

   +++

   @@ -1,4 +1,4 @@

   {
   -    "key": "one"
   +    "key": "two"
   }
   Wrote data to Zookeeper.


The ZooKeeper node must be created before this tool can be used so that
appropriate ACLs can be configured.


Direct access to ZooKeeper
--------------------------

If you're doing something more complicated with your data that the above tools
don't cover, you'll want to connect directly to ZooKeeper.

.. autofunction:: baseplate.lib.live_data.zookeeper_client_from_config
