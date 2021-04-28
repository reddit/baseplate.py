from wsgiref.simple_server import make_server

from pyramid.config import Configurator
from pyramid.view import view_config


@view_config(route_name="hello_world", renderer="json")
def hello_world(request):
    return {"Hello": "World"}


def make_wsgi_app():
    configurator = Configurator()
    configurator.add_route("hello_world", "/", request_method="GET")
    configurator.scan()
    return configurator.make_wsgi_app()


if __name__ == "__main__":
    app = make_wsgi_app()
    server = make_server("127.0.0.1", 9090, app)
    server.serve_forever()
