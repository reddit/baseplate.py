Observers --- Looking under the hood
====================================

In the previous chapter we took our basic Pyramid service and made it
compatible with :program:`baseplate-serve`. Now we'll start using Baseplate.py
inside the service itself so we can see what's going on while handling
requests.

Wire up the Baseplate object
----------------------------

The heart of Baseplate.py's framework is the :py:class:`~baseplate.Baseplate`
object. No matter what kind of service you're writing---Pyramid, Thrift,
etc.---this class works the same. The magic happens when we use one of
Baseplate.py's framework integrations to connect the two things together.
Let's do that in our service now.

.. literalinclude:: observed.py
   :language: python
   :emphasize-lines: 1-2, 13-14, 17

This is all we need to do to get basic observability in our service.
Line-by-line:

.. literalinclude:: observed.py
   :language: python
   :lines: 13

We create a :py:class:`~baseplate.Baseplate` object during application startup.

.. literalinclude:: observed.py
   :language: python
   :lines: 14

Then we call :py:meth:`~baseplate.Baseplate.configure_observers` and pass in
the application configuration. We'll talk more about this in a moment.

.. literalinclude:: observed.py
   :language: python
   :lines: 17

Finally we connect up with Pyramid's framework to integrate it all together.

We can now run our server again and make some requests to it to see what's
different.

.. code-block:: console
   :emphasize-lines: 2

   $ baseplate-serve --debug helloworld.ini
   1072:MainThread:baseplate:DEBUG:The following observers are unconfigured and won't run: metrics, tracing, error_reporter
   1072:MainThread:baseplate.server.runtime_monitor:INFO:No metrics client configured. Server metrics will not be sent.
   1072:MainThread:baseplate.server:INFO:Listening on ('127.0.0.1', 9090)
   1072:3776872808671626432:baseplate.server.wsgi:DEBUG:127.0.0.1 - - [2019-08-08 04:46:58] "GET / HTTP/1.1" 200 147 0.008789

It still works and things don't look too different. The first thing you'll see
is the ``observers are unconfigured`` line. This is there because we called
:py:meth:`~baseplate.Baseplate.configure_observers`. We did not add anything to
our configuration file so of course they're all unconfigured!

There is one other change even with those unconfigured observers. The log line
for the test request we sent no longer says ``DummyThread-1`` but instead has a
really long number in its place. That's the Trace ID of the request. You'll see
in the next section that when a single request causes multiple log lines,
they'll all have the same Trace ID which helps correlate them.

.. note::

   In fact, the Trace ID will be the same across all services involved in
   handling a single end-user request. We'll talk more about this in a later
   chapter.

That's sort of useful but we can do better. Next, let's configure another
observer to get more visibility.

Configure the metrics observer
------------------------------

After the previous section, our application is now wired up to use Baseplate.py
with Pyramid. Now we'll turn on an observer to see it in action.

One of the available observers sends metrics to `StatsD`_. We'll turn that on,
but since we don't actually have a StatsD running we'll leave it in debug mode
that just prints the metrics it would send out to the logs instead.

.. _`StatsD`: https://github.com/statsd/statsd

.. literalinclude:: observed.ini
   :language: ini
   :emphasize-lines: 4

To turn it on, we just add one line to our configuration file. This tells the
observer what base name it should use for the metrics it sends. Once we have
done that, we can start the server up again.

.. code-block:: console

   $ baseplate-serve --debug helloworld.ini
   1104:MainThread:baseplate:DEBUG:The following observers are unconfigured and won't run: tracing, error_reporter
   1104:MainThread:baseplate.server:INFO:Listening on ('127.0.0.1', 9090)
   1104:2115808718993382189:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.server.hello_world:3.53074|ms'
   1104:2115808718993382189:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.server.hello_world.success:1|c'
   1104:2115808718993382189:baseplate.server.wsgi:DEBUG:127.0.0.1 - - [2019-08-08 04:47:32] "GET / HTTP/1.1" 200 147 0.009720

If it worked right, ``metrics`` won't be listed as an unconfigured observer
anymore. Now when you make requests to your service you'll see a few extra log
lines that say ``Would send metric...``. These are the metrics the observer
would be sending if we had a StatsD server set up. Also note that the trace ID
is the same on all these log lines.

Since our service is super simple, we only get two metrics on each request. The
first is a timer that tracks how long the endpoint took to respond to the
request. The second metric increments a success or failure counter every time
the endpoint responds or crashes.

If you leave your server running long enough, you'll also see some extra
metrics appear periodically:

.. code-block:: console

   1104:Server Monitoring:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.runtime.reddit.PID1104.active_requests:0|g'
   1104:Server Monitoring:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.runtime.reddit.PID1104.gc.gen0.collections:154|g'
   1104:Server Monitoring:baseplate.lib.metrics:DEBUG:Would send metric b'helloworld.runtime.reddit.PID1104.gc.gen0.collected:8244|g'
   ...

These metrics come out of the server itself and track information that's not
specific to an individual request but rather about the overall health of the
service. This includes things like statistics from Python's garbage collector,
the state of any connection pools, and how many concurrent requests your
application is handling.

Summary
-------

We have integrated Baseplate.py's tools into our service and started seeing
some of the benefit of its observers. Our service is pretty simple still
though, it's about time it actually talks to something else. In the next
chapter, we'll add a database and see what that looks like.
