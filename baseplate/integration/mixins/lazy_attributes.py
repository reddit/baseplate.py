class LazyAttributesMixin(object):
    """A mixin that allows instances to add a lazily evaluated attribute
       through `add_lazy_property`.
    """

    # Called when an attribute is missing
    def __getattr__(self, name):
        if name not in self._lazy_attributes:
            return super(LazyAttributesMixin, self).__getattr__(name)
        prop_value = self._lazy_attributes[name](self)
        setattr(self, name, prop_value)
        return prop_value

    def add_lazy_attribute(self, name, callable):
        """Add a lazily evaluated attribute to this instance.

        :param str name: The name of the attribute.
        :param func callable: The function to invoke when the attribute is fist accessed.
            This function will receive the current context as its first argument.

        """
        self._lazy_attributes[name] = callable
