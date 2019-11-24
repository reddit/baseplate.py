from baseplate import Baseplate
from baseplate.clients.sqlalchemy import SQLAlchemySession
from baseplate.frameworks.pyramid import BaseplateConfigurator
from pyramid.config import Configurator
from pyramid.view import view_config


@view_config(route_name="hello_world", renderer="json")
def hello_world(request):
    result = request.db.execute("SELECT date('now');")
    return {"Hello": "World", "Now": result.scalar()}


def make_wsgi_app(app_config):
    baseplate = Baseplate(app_config)
    baseplate.configure_observers()
    baseplate.configure_context({"db": SQLAlchemySession()})

    configurator = Configurator(settings=app_config)
    configurator.include(BaseplateConfigurator(baseplate).includeme)
    configurator.add_route("hello_world", "/", request_method="GET")
    configurator.scan()
    return configurator.make_wsgi_app()
