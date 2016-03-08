==============
Message Queues
==============

Baseplate includes a :py:class:`~baseplate.message_queue.MessageQueue`
convenience class that wraps `POSIX Message queues`_. While not available
on all operating systems, POSIX queues are a simple and efficient medium
for Inter-Process Communication (IPC).

Minimal Example
---------------

Here's a minimal, artificial example of a separate producer and consumer
process pair (run the producer then the consumer):

.. code-block:: python

    # producer.py
    from baseplate.message_queue import MessageQueue

    # If the queue doesn't already exist, we'll create it.
    mq = MessageQueue(
        "/baseplate-testing", max_messages=1, max_message_size=1)
    message = "1"
    mq.put(message)
    print("Put Message: %s" % message)


.. code-block:: python

    # consumer.py
    from baseplate.message_queue import MessageQueue

    mq = MessageQueue(
        "/baseplate-testing", max_messages=1, max_message_size=1)
    # Unless a `timeout` kwarg is passed, this will block until
    # we can pop a message from the queue.
    message = mq.get()
    print("Get Message: %s" % message)

The ``/baseplate-testing`` value is the name of the queue. Queues names should
start with a forward slash, followed by one or more characters (but no
additional slashes).

Multiple processes can bind to the same queue by specifying the same queue
name.

.. _POSIX Message queues: http://man7.org/linux/man-pages/man7/mq_overview.7.html

Message Queue Default Limits
----------------------------

Most operating systems with POSIX queues include very low defaults for the
maximum message size and maximum queue depths. On Linux 2.6+, you can
list and check the values for these by running::

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
up seeing a vague ``ValueError`` exception. Here's an example::

    >>> from baseplate.message_queue import MessageQueue
    >>> mq = MessageQueue(
            "/over-the-limit", max_messages=11, max_message_size=8096)
    Traceback (most recent call last):
      File "<input>", line 2, in <module>
      File "/home/myuser/baseplate/baseplate/message_queue.py", line 83, in __init__
        max_message_size=max_message_size,
    ValueError: Invalid parameter(s)

Since the default value for ``/proc/sys/fs/mqueue/msg_max`` on Linux is 10,
our ``max_messages=11`` is invalid. You can raise these limits by doing
something like this as a privileged user::

    $ echo "50" > /proc/sys/fs/mqueue/msg_max
