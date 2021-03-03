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

.. literalinclude:: helloworld.py
   :language: python
   :emphasize-lines: 1-2, 13-14, 17

This is all we need to do to get basic observability in our service.
Line-by-line:

.. literalinclude:: helloworld.py
   :language: python
   :lines: 13

We create a :py:class:`~baseplate.Baseplate` object during application startup.

.. literalinclude:: helloworld.py
   :language: python
   :lines: 14

Then we call :py:meth:`~baseplate.Baseplate.configure_observers` and pass in
the application configuration. We'll talk more about this in a moment.

.. literalinclude:: helloworld.py
   :language: python
   :lines: 17

Finally we connect up with Pyramid's framework to integrate it all together.

We can now run our server again and make some requests to it to see what's
different.

.. code-block:: console
   :emphasize-lines: 2

   $ baseplate-serve --debug helloworld.ini
   {"message": "The following observers are unconfigured and won't run: metrics, tracing, sentry", ...
   {"message": "No metrics client configured. Server metrics will not be sent.", ...
   {"message": "Listening on ('127.0.0.1', 9090), PID:2308014", ...
   {"message": "127.0.0.1 - - [2021-03-02 15:08:15] \"GET / HTTP/1.1\" 200 145 0.002052", ...

It still works and things don't look too different. The first thing you'll see
is the ``observers are unconfigured`` line. This is there because we called
:py:meth:`~baseplate.Baseplate.configure_observers`. We did not add anything to
our configuration file so of course they're all unconfigured!

There is one other change even with those unconfigured observers. The log line
for the test request we sent now has a field called "traceID".  That's the
Trace ID of the request. You'll see in the next section that when a single
request causes multiple log lines, they'll all have the same Trace ID which
helps correlate them.

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

.. literalinclude:: helloworld.ini
   :language: ini
   :emphasize-lines: 4-5

This tells Baseplate to configure the :doc:`tagged metrics observer
</api/baseplate/observers/tagged_statsd>` and that it should log the metrics
it would send had we configured a destination. Once we have done that, we can
start the server up again.

.. code-block:: console

   $ baseplate-serve --debug helloworld.ini
   {"message": "The following observers are unconfigured and won't run: tracing, sentry", ...
   {"message": "Listening on ('127.0.0.1', 9090), PID:2310914", ...
   {"message": "Would send metric b'baseplate.server.latency,endpoint=hello_world:1.80316|ms'", ...
   {"message": "Would send metric b'baseplate.server.rate,endpoint=hello_world,success=True:1|c'", ...
   {"message": "127.0.0.1 - - [2021-03-02 15:10:34] \"GET / HTTP/1.1\" 200 145 0.004433", ...

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

   {"message": "Would send metric b'runtime.open_connections,hostname=reddit,PID=2311569:0|g'", ...
   {"message": "Would send metric b'runtime.active_requests,hostname=reddit,PID=2311569:0|g'", ...
   {"message": "Would send metric b'runtime.gc.collections,hostname=reddit,PID=2311569,generation=0:110|g'" ...
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
