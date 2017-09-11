from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging

from .providers import parse_experiment
from .. import config
from .._compat import iteritems
from ..context import ContextFactory
from ..events import Event, EventTooLargeError, EventQueueFullError
from ..file_watcher import FileWatcher, WatchedFileNotAvailableError


logger = logging.getLogger(__name__)


class ExperimentsContextFactory(ContextFactory):
    """Experiment client context factory

    This factory will attach a new :py:class:`baseplate.experiments.Experiments`
    to an attribute on the :term:`context object`.
    """
    def __init__(self, path, event_queue):
        """ExperimentContextFactory constructor

        :param str path: Path to the experiment config file.
        :param baseplate.events.EventQueue event_queue: Event queue used for
            logging bucketing events to the event pipeline.  You can set this
            to None to disable bucketing events entirely.
        """
        self._filewatcher = FileWatcher(path, json.load)
        self._event_queue = event_queue

    def make_object_for_context(self, name, server_span):
        return Experiments(
            config_watcher=self._filewatcher,
            event_queue=self._event_queue,
            server_span=server_span,
            context_name=name,
        )


class Experiments(object):
    """Access to experiments with automatic refresh when changed.

    This experiments client allows access to the experiments cached on disk
    by the experiment config fetcher daemon.  It will automatically reload
    the cache when changed.  This client also handles logging bucketing events
    to the event pipeline when it is determined that the request is part of an
    active variant.
    """

    def __init__(self, config_watcher, event_queue, server_span, context_name):
        self._config_watcher = config_watcher
        self._event_queue = event_queue
        self._span = server_span
        self._context_name = context_name
        self._already_bucketed = set()
        self._experiment_cache = {}

    def _get_config(self, name):
        try:
            config_data = self._config_watcher.get_data()
            return config_data[name]
        except WatchedFileNotAvailableError as exc:
            logger.warning("Experiment config unavailable: %s", str(exc))
            return
        except KeyError:
            logger.warning(
                "Experiment <%r> not found in experiment config",
                name,
            )
            return
        except TypeError as exc:
            logger.warning("Could not load experiment config: %s", str(exc))
            return

    def _get_experiment(self, name):
        if name not in self._experiment_cache:
            experiment_config = self._get_config(name)
            if not experiment_config:
                experiment = None
            else:
                experiment = parse_experiment(experiment_config)
            self._experiment_cache[name] = experiment
        return self._experiment_cache[name]

    def variant(self, name, bucketing_event_override=None,
                extra_event_fields=None, **kwargs):
        """Which variant, if any, is active.

        If a variant is active, a bucketing event will be logged to the event
        pipeline unless any one of the following conditions are met:

        1. bucketing_event_override is set to False.
        2. The experiment specified by "name" explicitly disables bucketing
           events.
        3. We have already logged a bucketing event for the value specified by
           experiment.get_unique_id(\*\*kwargs) within the current
           request.

        Since checking the status an experiment will fire a bucketing event, it
        is best to only check the variant when you are making the decision that
        will expose the experiment to the user.  If you absolutely must check
        the status of an experiment before you are sure that the experiment
        will be exposed to the user, you can use `bucketing_event_override` to
        disabled bucketing events for that check.

        :param str name: Name of the experiment you want to run.
        :param bool bucketing_event_override: (Optional) Set if you need to
            override the default behavior for sending bucketing events.  This
            parameter should be set sparingly as it breaks the assumption that
            you will fire a bucketing event when you first check the state of
            an experiment.  If set to False, will never send a bucketing event.
            If set to None, no override will be applied.  Set to None by
            default.  Note that setting bucketing_event_override to True has no
            effect, it will behave the same as when it is set to None.
        :param dict extra_event_fields: (Optional) Any extra fields you want to
            add to the bucketing event.
        :param kwargs:  Arguments that will be passed to experiment.variant to
            determine bucketing, targeting, and overrides.

        :rtype: :py:class:`str`
        :return: Variant name if a variant is active, None otherwise.
        """
        experiment = self._get_experiment(name)

        if experiment is None:
            return None

        span_name = "{}.{}".format(self._context_name, "variant")
        with self._span.make_child(span_name, local=True, component_name=name):
            variant = experiment.variant(**kwargs)

        bucketing_id = experiment.get_unique_id(**kwargs)
        do_log = True

        if not bucketing_id:
            do_log = False

        if variant is None:
            do_log = False

        if bucketing_event_override is False:
            do_log = False

        if bucketing_id and bucketing_id in self._already_bucketed:
            do_log = False

        do_log = do_log and experiment.should_log_bucketing()

        if do_log:
            assert bucketing_id
            self._log_bucketing_event(experiment, variant, extra_event_fields)
            self._already_bucketed.add(bucketing_id)

        return variant

    def _log_bucketing_event(self, experiment, variant, extra_event_fields):
        if not self._event_queue:
            return

        extra_event_fields = extra_event_fields or {}

        event = Event("bucketing_events", "bucket")
        for field, value in iteritems(extra_event_fields):
            event.set_field(field, value)

        event.set_field("variant", variant)
        event.set_field("experiment_id", experiment.id)
        event.set_field("experiment_name", experiment.name)
        event.set_field("owner", experiment.owner)

        version = getattr(experiment, "version", None)
        if version:
            event.set_field("version", version)

        span_name = "{}.{}".format(self._context_name, "events")
        with self._span.make_child(span_name) as child_span:
            try:
                self._event_queue.put(event)
            except EventTooLargeError as exc:
                logger.warning(
                    "The event payload was too large for the event queue."
                )
                child_span.set_tag("error", True)
                child_span.log("error.object", exc)
            except EventQueueFullError as exc:
                logger.warning("The event queue is full.")
                child_span.set_tag("error", True)
                child_span.log("error.object", exc)


def experiments_client_from_config(app_config, event_queue):
    """Configure and return an :py:class:`ExperimentsContextFactory` object.

    This expects one configuration option:

    ``experiments.path``
        The path to the experiment config file generated by the experiment
        config fetcher daemon.

    :param dict raw_config: The application configuration which should have
        settings for the experiments client.
    :param baseplate.events.EventQueue event_queue: The EventQueue to be used
        to log bucketing events.
    :rtype: :py:class:`ExperimentsContextFactory`

    """
    cfg = config.parse_config(app_config, {
        "experiments": {
            "path": config.Optional(config.String, default="/var/local/experiments.json"),
        },
    })
    # pylint: disable=maybe-no-member
    return ExperimentsContextFactory(cfg.experiments.path, event_queue)
