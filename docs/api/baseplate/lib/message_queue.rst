``baseplate.lib.message_queue``
===============================

This module provides a thin wrapper around POSIX Message queues.

.. note::

   This implementation uses `POSIX Message queues`_ and is not portable to
   all operating systems.

   There are also various limits on the sizes of queues:

   * The ``msgqueue`` ``rlimit`` limits the amount of space the user can use on
     message queues.
   * The ``fs.mqueue.msg_max`` and ``fs.mqueue.msgsize_max`` sysctls limit
     the maximum number of messages and the maximum size of each message
     which a queue can be configured to have.

.. _POSIX Message queues: http://man7.org/linux/man-pages/man7/mq_overview.7.html

Minimal Example
---------------

Here's a minimal, artificial example of a separate producer and consumer
process pair (run the producer then the consumer):

.. testcode::

    # producer.py
    from baseplate.lib.message_queue import MessageQueue

    # If the queue doesn't already exist, we'll create it.
    mq = MessageQueue(
        "/baseplate-testing", max_messages=1, max_message_size=1)
    message = "1"
    mq.put(message)
    print("Put Message: %s" % message)

You should see:

.. testoutput::

   Put Message: 1

After running the producer once, we have a single message pushed on to our
POSIX message queue. Next up, run the consumer:

.. testcode::

    # consumer.py
    from baseplate.lib.message_queue import MessageQueue

    mq = MessageQueue(
        "/baseplate-testing", max_messages=1, max_message_size=1)
    # Unless a `timeout` kwarg is passed, this will block until
    # we can pop a message from the queue.
    message = mq.get()
    print("Get Message: %s" % message.decode())

You'll end up seeing:

.. testoutput::

   Get Message: 1

The ``/baseplate-testing`` value is the name of the queue. Queues names should
start with a forward slash, followed by one or more characters (but no
additional slashes).

Multiple processes can bind to the same queue by specifying the same queue
name.

Message Queue Default Limits
----------------------------

Most operating systems with POSIX queues include very low defaults for the
maximum message size and maximum queue depths. On Linux 2.6+, you can
list and check the values for these by running:

.. code-block:: console

    $ ls /proc/sys/fs/mqueue/
    msg_default  msg_max  msgsize_default  msgsize_max  queues_max
    $ cat /proc/sys/fs/mqueue/msgsize_max
    8192

Explaining these in detail is outside the scope of this document, so we'll
refer you to `POSIX Message queues`_ (or ``man 7 mq_overview``) for detailed
instructions on what these mean.

Gotchas
-------

If you attempt to create a POSIX Queue where one of your provided values is
over the limits defined under ``/proc/sys/fs/mqueue/``, you'll probably end
up seeing a vague ``ValueError`` exception. Here's an example:

.. code-block:: pycon

    >>> from baseplate.lib.message_queue import MessageQueue
    >>> mq = MessageQueue(
            "/over-the-limit", max_messages=11, max_message_size=8096)
    Traceback (most recent call last):
      File "<input>", line 2, in <module>
      File "/home/myuser/baseplate/baseplate/lib/message_queue.py", line 83, in __init__
        max_message_size=max_message_size,
    ValueError: Invalid parameter(s)

Since the default value for ``/proc/sys/fs/mqueue/msg_max`` on Linux is 10,
our ``max_messages=11`` is invalid. You can raise these limits by doing
something like this as a privileged user:

.. code-block:: console

    $ echo "50" > /proc/sys/fs/mqueue/msg_max


CLI Usage
---------

The `message_queue` module can also be run as a command-line tool to consume,
log, and discard messages from a given queue:

.. code-block:: console

    $ python -m baseplate.lib.message_queue --read /queue

or to write arbitrary messages to the queue:

.. code-block:: console

    $ echo hello! | python -m baseplate.lib.message_queue --write /queue

See ``--help`` for more info.

``baseplate.lib.message_queue``
-------------------------------

.. automodule:: baseplate.lib.message_queue

.. autoclass:: MessageQueue
   :members:


Exceptions
----------

.. autoexception:: MessageQueueError

.. autoexception:: TimedOutError
