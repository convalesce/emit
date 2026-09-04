"""
Best-effort JSON shaping of a tool's own objects.

Deliberately shallow and forgiving. This is not modelling anything: it only
gets a tool's object across as JSON so the receiver can interpret it.
Anything unserialisable is stringified rather than dropped, because a partial
observation is worth more than none, and nothing here may raise into the
customer's pipeline.

The budgets below are not tidiness. A Prefect `Flow` reaches its task runner,
then that runner's logger, then the logging manager, then every logger in the
process with its handlers and streams: walking one real flow run produced a
256 MB payload before these limits existed. A forwarder that can allocate
that inside a customer's worker is worse than one that reports nothing.

Import as:

import convalesce_emit.serialize as ceserial
"""

import dataclasses
import io
import logging
import threading
import types
from typing import Any, Optional, Set, Tuple

_LOG = logging.getLogger(__name__)

_SCALARS = (bool, int, float, str)
# Tried in order. Pydantic models answer to the first, Airflow and Dagster
# objects usually to the second or third, and namedtuples to the last.
_DUMP_METHODS = ("model_dump", "dict", "to_dict", "_asdict")

# Deep enough for the payloads these tools actually produce. Anything past it
# is machinery, not state.
_MAX_DEPTH = 4
# Total values written. Caps breadth, which depth alone does not: one object
# holding a registry of thousands is shallow and still enormous.
_MAX_NODES = 2000
_MAX_ITEMS = 200
_MAX_STRING = 4096

# Attribute names that lead out of the tool's own state and into the runtime.
_SKIP_NAMES = frozenset(
    {
        "filters",
        "handlers",
        "lock",
        "log",
        "logger",
        "loggerdict",
        "manager",
        "parent",
        "root",
        "stream",
    }
)

# Values that are plumbing wherever they appear.
_SKIP_TYPES: Tuple[type, ...] = (
    logging.Logger,
    logging.Handler,
    logging.Filterer,
    io.IOBase,
    types.ModuleType,
    types.FunctionType,
    types.MethodType,
    types.TracebackType,
    threading.Thread,
    type,
    BaseException,
)


# #############################################################################
# _Budget
# #############################################################################


@dataclasses.dataclass
class _Budget:
    """
    How much of one payload is left to spend.

    :param nodes: values still allowed before the walk stops
    :param seen: ids already visited, so a cycle terminates
    """

    nodes: int = _MAX_NODES
    seen: Set[int] = dataclasses.field(default_factory=set)

    def spend(self) -> bool:
        """
        Take one node from the budget.

        :return: whether there was anything left to take
        """
        self.nodes -= 1
        return self.nodes > 0


def dump(  # pylint: disable=too-many-return-statements
    obj: Any, *, budget: Optional[_Budget] = None, depth: int = 0
) -> Any:
    """
    Convert a tool's object into something JSON can carry.

    The return type is `Any` because both ends of this are boundaries: the
    input came from a tool we do not own, and the output goes to a receiver
    that treats it as opaque.

    :param obj: whatever the tool handed the callback
    :param budget: remaining size allowance, created on the first call
    :param depth: recursion depth
    :return: a JSON-encodable equivalent, truncated where a budget ran out
    """
    if budget is None:
        budget = _Budget()
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        return (
            obj
            if len(obj) <= _MAX_STRING
            else obj[:_MAX_STRING] + "...(truncated)"
        )
    if isinstance(obj, _SKIP_TYPES):
        return _describe(obj)
    if not budget.spend():
        return "...(truncated: size limit)"
    if depth >= _MAX_DEPTH:
        return _describe(obj)
    # Cycles are common once an object graph includes a parent pointer.
    marker = id(obj)
    if marker in budget.seen:
        return "...(cycle)"
    budget.seen.add(marker)
    try:
        return _dump_container(obj, budget, depth)
    finally:
        budget.seen.discard(marker)


def _dump_container(obj: Any, budget: _Budget, depth: int) -> Any:
    """
    Walk one level of a container or object.

    :param obj: the value being walked
    :param budget: remaining size allowance
    :param depth: current recursion depth
    :return: a JSON-encodable equivalent
    """
    nxt = depth + 1
    if isinstance(obj, dict):
        out = {}
        for key, value in list(obj.items())[:_MAX_ITEMS]:
            name = str(key)
            if name.lower() in _SKIP_NAMES:
                continue
            out[name] = dump(value, budget=budget, depth=nxt)
        return out
    # Namedtuples are tuples, so this has to come first or a Dagster run
    # arrives as an anonymous array with every field name lost.
    dumped = _try_dump_methods(obj, budget, nxt)
    if dumped is not _UNSET:
        return dumped
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [
            dump(v, budget=budget, depth=nxt) for v in list(obj)[:_MAX_ITEMS]
        ]
    if dumped is not _UNSET:
        return dumped
    data = getattr(obj, "__dict__", None)
    if isinstance(data, dict):
        public = {}
        for key, value in list(data.items())[:_MAX_ITEMS]:
            name = str(key)
            # Leading underscores are the tool's internals, not its state.
            if name.startswith("_") or name.lower() in _SKIP_NAMES:
                continue
            public[name] = dump(value, budget=budget, depth=nxt)
        # An empty mapping tells the receiver nothing; the object's own
        # description at least names what it was.
        if public:
            return public
    return _describe(obj)


def _describe(obj: Any) -> str:
    """
    Name a value we are not going to walk into.

    :param obj: the value being summarised
    :return: a short, safe description
    """
    try:
        text = str(obj)
    except Exception:  # pylint: disable=broad-exception-caught
        # An object whose __str__ raises still has a type.
        return f"<{type(obj).__name__}>"
    if len(text) > _MAX_STRING:
        text = text[:_MAX_STRING] + "...(truncated)"
    return text


# Sentinel: None is a legitimate result of a tool's own dump method.
_UNSET = object()


def _try_dump_methods(obj: Any, budget: _Budget, depth: int) -> Any:
    """
    Ask the object to describe itself.

    :param obj: the object to try
    :param budget: remaining size allowance
    :param depth: recursion depth to pass on
    :return: the dumped value, or `_UNSET` if no method worked
    """
    for attr in _DUMP_METHODS:
        method = getattr(obj, attr, None)
        if not callable(method):
            continue
        try:
            return dump(method(), budget=budget, depth=depth)
        except Exception:  # pylint: disable=broad-exception-caught
            # A tool's own serialiser failing is not our problem to solve;
            # fall through and describe the object some other way.
            break
    return _UNSET
