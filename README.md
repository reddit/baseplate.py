# baseplate

[![Build Status](https://travis-ci.org/reddit/baseplate.svg?branch=master)](https://travis-ci.org/reddit/baseplate)

<img src="docs/images/baseplate.png" alt="A LEGO baseplate." width="300px" align="right">

Baseplate is a library to build services on. Its goal is to provide all the
common things a service needs with as few surprises as possible, including:

* compatibility with Python 2.7 and Python 3.4+
* transparent diagnostic information collection (metrics, tracing, logging)
* configuration parsing
* gevent-based Thrift and WSGI servers meant to run under [Einhorn]
* and various helper libraries like a Thrift client pool

Check out the [quick start] to get going quickly, or read the [full docs].

[quick start]: http://reddit.github.io/baseplate/quickstart.html
[full docs]: http://reddit.github.io/baseplate/index.html
[Einhorn]: https://github.com/stripe/einhorn
