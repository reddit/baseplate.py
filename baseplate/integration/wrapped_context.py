import logging


class WrappedRequestContext(object):
    def __init__(self, context, trace=None):
        self.__dict__['_context'] = context
        self.__dict__['logger'] = logging.getLogger(self.__class__.__name__)

    def __getattr__(self, attr):
        return getattr(self._context, attr)

    def __setattr__(self, attr, value):
        if attr in self._context.__dict__:
            self._context.__setattr__(attr, value)
        else:
            self.logger.debug("Assigning new attr=%s to wrapped request context.")
            super(WrappedRequestContext, self).__setattr__(attr, value)

    def clone(self):
        new_wrapped_context = WrappedRequestContext(self._context)
        return new_wrapped_context
