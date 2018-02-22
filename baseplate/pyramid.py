"""Helpers for adding routes to the pyramid configurator.

It's straightforward to add routes with
:py:meth:`pyramid.config.Configurator.add_route` and
:py:meth:`pyramid.config.Configurator.add_view` but this module provides
helpers to ensure that the spans and metrics derived from those routes have a
standard structure.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import re

from pyramid.httpexceptions import (
    HTTPError,
    HTTPNotFound,
)


class CORSController(object):
    CORS_GLOB_REPLACEMENT = r'[A-Za-z0-9\-\.]+'

    def __init__(self, request_methods, handler=None, allowed_origins=None):
        self.request_methods_str = ', '.join(request_methods)
        self.handler = handler
        self.allowed_origins = allowed_origins or []

    def _is_request_from_allowed_origin(self, origin):
        if not origin:
            return False

        for allowed_origin in self.allowed_origins:
            regex = r'\A'
            regex += self.CORS_GLOB_REPLACEMENT.join(
                re.escape(string) for string in allowed_origin.split('*')
            )
            # If we allow a host we allow requests from any of its hosts
            regex += r'(:\d+)?\Z'

            if re.match(regex, origin):
                return True

        return False

    def handle_cors(self, request):
        request_origin = request.headers.get('Origin')
        if self._is_request_from_allowed_origin(request_origin):
            request.response.headers.update({
                'Access-Control-Allow-Origin': str(request_origin),
                'Access-Control-Allow-Methods': self.request_methods_str,
                'Access-Control-Allow-Headers': 'authorization,Content-Type',
                'Access-Control-Allow-Credentials': 'true',
                'Access-Control-Max-Age': '86400',
                'Vary': 'Origin',
            })

        if self.handler:
            return self.handler(request)


def add_route(configurator, handlers, pattern, cors_allowed_origins=None, renderer='json'):
    """Add a route and view to the configurator.

    Baseplate server spans (and metrics) are named using the route name, so
    ensuring that the route names have a standard structure can be useful.

    ``handlers`` must be a dict of request method -> handler, e.g.::

        my_special_controller = SpecialController()
        handlers = {
          'GET': my_special_controller.GET_things,
          'POST': my_special_controller.POST_things,
        }

    The route name for handler method ``SpecialController.POST_things()`` is
    'special.POST_things'.

    When using CORS an OPTIONS route is also created with a route name like
    'special.OPTIONS_things'.

    The controller class name MUST end with 'Controller'.
    The handler method name MUST begin with the HTTP verb.

    :param pyramid.config.Configurator configurator: The pyramid configurator.
    :param dict handler: Map of request method name to handler function.
    :param pattern str: The pattern of the route. If the pattern doesn't match
        the current URL, route matching continues.
        See `Pyramid Route Pattern Syntax`_.
    :param tuple cors_allowed_origins: Which origin sites are allowed for CORS.
    :param str renderer: Pylons renderer. See `Pyramid Docs`_.

    .. _Pyramid Route Pattern Syntax:
        https://docs.pylonsproject.org/projects/pyramid/en/latest/narr/urldispatch.html#route-pattern-syntax
    .. _Pyramid Docs:
        https://docs.pylonsproject.org/projects/pyramid/en/latest/api/config.html

    """
    controller_names = set()
    verbless_handler_names = set()
    for request_method, handler in handlers.iteritems():
        controller_name = handler.im_class.__name__.lower()
        assert controller_name.endswith('controller')
        assert handler.__name__.startswith(request_method + '_')
        verbless_handler_name = handler.__name__[len(request_method + '_'):]
        controller_names.add(controller_name)
        verbless_handler_names.add(verbless_handler_name)

    assert len(verbless_handler_names) == 1
    verbless_handler_name = list(verbless_handler_names)[0]

    assert len(controller_names) == 1
    controller_name = list(controller_names)[0]

    if cors_allowed_origins:
        cors_request_methods = handlers.keys()
    else:
        cors_request_methods = []

    for request_method, handler in handlers.iteritems():
        route_name = '{controller}.{verb}_{handler}'.format(
            controller=controller_name[:-len('controller')],
            verb=request_method,
            handler=verbless_handler_name,
        )

        configurator.add_route(
            name=route_name,
            pattern=pattern,
            request_method=request_method,
        )

        if cors_allowed_origins:
            # Decorate this handler so it will respond with CORS headers
            cors_view = CORSController(
                request_methods=cors_request_methods,
                handler=handler,
                allowed_origins=cors_allowed_origins,
            ).handle_cors
            configurator.add_view(
                view=cors_view,
                route_name=route_name,
                renderer=renderer,
            )
        else:
            configurator.add_view(
                view=handler,
                route_name=route_name,
                renderer=renderer,
            )

    if cors_allowed_origins:
        # Add a route and view for the OPTIONS endpoint for this path and all
        # its allowed request methods
        options_route_name = '{controller}.OPTIONS_{handler}'.format(
            controller=controller_name[:-len('controller')],
            verb=request_method,
            handler=verbless_handler_name,
        )
        cors_options_view = CORSController(
            request_methods=cors_request_methods,
            allowed_origins=cors_allowed_origins,
        ).handle_cors
        configurator.add_route(
            name=options_route_name,
            pattern=pattern,
            request_method='OPTIONS',
        )
        configurator.add_view(
            view=cors_options_view,
            route_name=options_route_name,
            renderer=renderer,
        )


def add_not_found_handler(configurator, renderer='json', content_type='application/json'):
    """Add a catch-all route for unmatched requests.

    Pyramid will automatically respond with a 404 for unmatched routes but we
    want to set the response content type and increment the error counter. The
    error counter is at 'error.http.unknown.404'.

    :param pyramid.config.Configurator configurator: The pyramid configurator.
    :param str renderer:
    :param str content_type:

    """

    def _notfound_handler(exc, request):
        response = HTTPNotFound()
        response.content_type = content_type
        body = {
            'code': 404,
            'detail': request.path,
            'reason': 'NOT_FOUND',
        }
        response.body = json.dumps(body, indent=2)

        request.metrics.counter('error.http.unknown.404').increment()

        return response

    configurator.add_route(
        name='notfound',
        pattern='/*subpath',
    )
    configurator.add_view(
        view=_notfound_handler,
        route_name='notfound',
        renderer=renderer,
    )


def add_error_handler(configurator, renderer='json', content_type='application/json'):
    """Add a handler for exceptions raised within the handler method.

    Set the response content type and increment the error counter. The error
    counter is at 'error.http.{ROUTE_NAME}.{HTTP_ERROR_CODE}'.

    :param pyramid.config.Configurator configurator: The pyramid configurator.
    :param str renderer:
    :param str content_type:

    """

    def _http_error_to_json(exc, request):
        request.response.content_type = content_type
        request.response.status_code = exc.code
        body = {
            'code': exc.code,
            'reason': exc.title.replace(' ', '_').upper(),
        }
        if exc.detail:
            body['detail'] = exc.detail
        if exc.comment:
            body['comment'] = exc.comment
        request.response.body = json.dumps(body, indent=2)

        if request.matched_route:
            name = 'error.http.%s.%s' % (exc.code, request.matched_route.name)
        else:
            name = 'error.http.nomatch.%s' % exc.code
        request.metrics.counter(name).increment()

        return request.response

    configurator.add_view(
        view=_http_error_to_json,
        context=HTTPError,
        renderer=renderer,
    )
