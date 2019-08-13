from pyramid.config import Configurator
from pyramid.view import view_config


@view_config(route_name="hello_world", renderer="json")
def hello_world(request):
    return {"Hello": "World"}


def make_wsgi_app(app_config):
    configurator = Configurator(settings=app_config)
    configurator.add_route("hello_world", "/", request_method="GET")
    configurator.scan()
    return configurator.make_wsgi_app()
