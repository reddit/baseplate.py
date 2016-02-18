baseplate.message_queue
=======================

.. automodule:: baseplate.message_queue

.. note::

   This implementation uses POSIX message queues and is not portable to
   all operating systems.

   There are also various limits on the sizes of queues:

   * The ``msgqueue`` rlimit limits the amount of space the user can use on
     message queues.
   * The ``fs.mqueue.msg_max`` and ``fs.mqueue.msgsize_max`` sysctls limit the
     maximum number of messages and the maximum size of each message respectively
     which a queue can be configured to have.


.. autoclass:: MessageQueue
   :members:


Exceptions
----------

.. autoexception:: MessageQueueError

.. autoexception:: TimedOutError
