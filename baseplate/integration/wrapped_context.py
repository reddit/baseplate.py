import logging


class WrappedRequestContext(object):
    """A class for wrapping request contexts to add custom attributes.

    This class is used to wrap framework request contexts in order to
    shadow certain attributes like Baseplate-included context clients
    within Baseplate integration code without modifying the underlying
    context. Users can still access the underlying context through
    the standard getattr/setattr interface.
    """
    def __init__(self, context, trace=None):
        self.__dict__['_context'] = context
        self.__dict__['logger'] = logging.getLogger(self.__class__.__name__)

    def __getattr__(self, attr):
        return getattr(self._context, attr)

    def __setattr__(self, attr, value):
        self._context.__setattr__(attr, value)

    def shadow_context_attr(self, attr, value):
        """Explicit method for shadowing Baseplate-specific context attributes.

        This should be used for adding/modifying context attributes
        like context clients and traces when you don't want to change the
        underlying framework context. This is useful for manipulating
        local-span-aware attributes.
        """
        super(WrappedRequestContext, self).__setattr__(attr, value)

    def clone(self):
        new_wrapped_context = WrappedRequestContext(self._context)
        return new_wrapped_context
