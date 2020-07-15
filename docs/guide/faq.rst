Frequently Asked Questions
==========================

What do I do about "Metrics batch of N bytes is too large to send"?
-------------------------------------------------------------------

As your application processes a request, it does various actions that get
counted and timed. Baseplate.py batches up these metrics and sends them to the
metrics aggregator at the end of each request. The metrics are sent as a single
UDP datagram that has a finite maximum size (the exact amount depending on the
server) that is sufficiently large for normal purposes.

Seeing this error generally means that the application generated a *lot* of
metrics during the processing of that request. Since requests are meant to be
short lived, this indicates that the application is doing something
pathological in that request; a common example is making queries to a database
in a loop.

The best course of action is to dig into the application and reduce the amount
of work done in a given request by e.g. batching up those queries-in-a-loop
into fewer round trips to the database. This has the nice side-effect of
speeding up your application too!  To get you started, the "batch is too large"
error message also contains a list of the top counters in the oversized batch.
For example, if you see something like
``myservice.clients.foo_service.do_bar=9001`` that means you called the
``do_bar()`` method on ``foo_service`` over 9,000 times!

.. note:: For cron jobs or other non-server usages of Baseplate.py, you may
   need to break up your work into smaller units. For example, if your cron job
   processes a CSV file of 10,000 records you could create a server span for
   each record rather than one for the whole job.

Because this does not usually come up outside of legitimate performance issues
in the application, there are currently no plans to automatically flush very
large batches of metrics (which would silently mask performance issues like
this).
