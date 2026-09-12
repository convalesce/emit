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
import enum
import io
import logging
import math
import threading
import types
from typing import Any, Dict, FrozenSet, Optional, Set, Tuple

_LOG = logging.getLogger(__name__)

_SCALARS = (bool, int, float, str)
# Tried in order. Pydantic models answer to the first, Airflow and Dagster
# objects usually to the second or third, Great Expectations results to the
# fourth, and namedtuples to the last.
_DUMP_METHODS = ("model_dump", "dict", "to_dict", "to_json_dict", "_asdict")

# Levels of nesting walked before a value is described rather than opened.
# One level is one mapping or object; a list is transparent, since it holds
# more of the same thing rather than stepping into anything.
#
# Ten, because the deepest thing these tools nest sits at eight: a Great
# Expectations checkpoint result arrives as a keyword argument (2), dumps to
# a mapping of validation results (3, 4), each dumping to a list of
# expectation results (5), each holding an expectation config (6) whose
# kwargs (7) name the column (8). What keeps a payload bounded is the node
# budget and the skip lists below, not this; depth only stops pathological
# nesting from walking forever.
_MAX_DEPTH = 10
# Total values written. Caps breadth, which depth alone does not: one object
# holding a registry of thousands is shallow and still enormous. Sized for a
# real DAG: one Airflow task event carries the whole DAG, at about sixty
# values per task, and this leaves room for a few hundred tasks.
_MAX_NODES = 20_000
_MAX_ITEMS = 1000
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

# Names that mean a frame wherever they appear, for a frame this walker
# cannot measure: no shape, no schema. Checked by name as well as by shape
# because a frame's own `to_dict` returns every row and its `__str__` prints
# them, so neither may be reached. Only unambiguous names belong here: a key
# called `rows` is usually a row count.
_DATA_NAMES = frozenset({"dataframe", "data_frame", "df"})

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
    :param summarise: field names to name rather than walk into
    """

    nodes: int = _MAX_NODES
    seen: Set[int] = dataclasses.field(default_factory=set)
    summarise: FrozenSet[str] = frozenset()

    def spend(self) -> bool:
        """
        Take one node from the budget.

        :return: whether there was anything left to take
        """
        self.nodes -= 1
        return self.nodes > 0


def dump(  # pylint: disable=too-many-return-statements
    obj: Any,
    *,
    budget: Optional[_Budget] = None,
    depth: int = 0,
    summarise: FrozenSet[str] = frozenset(),
) -> Any:
    """
    Convert a tool's object into something JSON can carry.

    The return type is `Any` because both ends of this are boundaries: the
    input came from a tool we do not own, and the output goes to a receiver
    that treats it as opaque.

    :param obj: whatever the tool handed the callback
    :param budget: remaining size allowance, created on the first call
    :param depth: recursion depth
    :param summarise: fields the tool knows are duplication, named rather
        than walked; a plugin passes what its own tool duplicates
    :return: a JSON-encodable equivalent, truncated where a budget ran out
    """
    if budget is None:
        budget = _Budget(summarise=frozenset(name.lower() for name in summarise))
    if obj is None or isinstance(obj, (bool, int)):
        return obj
    if isinstance(obj, float):
        # JSON has no NaN or Infinity. Python writes them anyway and a strict
        # parser refuses the batch, which loses every observation in it.
        return obj if math.isfinite(obj) else None
    if isinstance(obj, str):
        return (
            obj
            if len(obj) <= _MAX_STRING
            else obj[:_MAX_STRING] + "...(truncated)"
        )
    if isinstance(obj, enum.Enum):
        # The member's value is the tool's state; its name is Python's.
        return dump(obj.value, budget=budget, depth=depth)
    if isinstance(obj, _SKIP_TYPES):
        return _describe(obj)
    if _is_bulk_data(obj):
        return _describe_bulk_data(obj)
    if not budget.spend():
        return "...(truncated: size limit)"
    if isinstance(obj, (list, tuple, set, frozenset)) and not _is_namedtuple(
        obj
    ):
        # Transparent: the items are at the same depth as the list.
        return [
            dump(v, budget=budget, depth=depth) for v in list(obj)[:_MAX_ITEMS]
        ]
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
        return _dump_mapping(obj, budget, nxt)
    # An object and the mapping it dumps to are one level, not two: the
    # mapping is the object, described by the tool's own method.
    dumped = _try_dump_methods(obj, budget, depth)
    if dumped is not _UNSET:
        return dumped
    fields = _dump_fields(obj, budget, nxt)
    if fields is not _UNSET:
        return fields
    data = getattr(obj, "__dict__", None)
    if isinstance(data, dict):
        public = {}
        for key, value in list(data.items())[:_MAX_ITEMS]:
            name = str(key)
            if name.lower() in _SKIP_NAMES:
                continue
            # Leading underscores are the tool's internals, not its state,
            # unless the class reads one back through a property of the
            # public name: then the property is the state and the private
            # attribute its storage. Airflow keeps a DAG's id, a run's state
            # and, before 2.10, a task's try number that way.
            if name.startswith("_"):
                exposed = name.lstrip("_")
                if (
                    exposed
                    and exposed not in data
                    and exposed.lower() not in _SKIP_NAMES
                    and _has_property(obj, exposed)
                ):
                    public[exposed] = _dump_property(obj, exposed, budget, nxt)
                continue
            if name.lower() in budget.summarise and not _is_scalar(value):
                public[name] = _describe(value)
                continue
            if name.lower() in _DATA_NAMES and not _is_scalar(value):
                public[name] = _describe_bulk_data(value)
                continue
            public[name] = dump(value, budget=budget, depth=nxt)
        # An empty mapping tells the receiver nothing; the object's own
        # description at least names what it was.
        if public:
            return public
    return _describe(obj)


def _dump_mapping(obj: Dict[Any, Any], budget: _Budget, depth: int) -> Any:
    """
    Walk one mapping.

    :param obj: the mapping being walked
    :param budget: remaining size allowance
    :param depth: depth for the values
    :return: a JSON-encodable mapping keyed by strings
    """
    out = {}
    for key, value in list(obj.items())[:_MAX_ITEMS]:
        # Keys are whatever the tool used; JSON wants strings. A Great
        # Expectations checkpoint keys its results by identifier objects.
        name = key if isinstance(key, str) else _describe(key)
        lowered = name.lower()
        if lowered in _SKIP_NAMES:
            continue
        if lowered in budget.summarise and not _is_scalar(value):
            out[name] = _describe(value)
            continue
        if lowered in _DATA_NAMES and not _is_scalar(value):
            out[name] = _describe_bulk_data(value)
            continue
        out[name] = dump(value, budget=budget, depth=depth)
    return out


def _dump_fields(obj: Any, budget: _Budget, depth: int) -> Any:
    """
    Read an object that declares its own field names.

    Namedtuples and Dagster's `@record` classes both carry `_fields`, and a
    record keeps nothing in `__dict__`: a Dagster job snapshot walked by
    attribute is empty, and its `_asdict` raises "Iteration is not allowed",
    so without this the whole snapshot arrives as its repr.

    :param obj: the object being walked
    :param budget: remaining size allowance
    :param depth: depth for the values
    :return: the fields as a mapping, or `_UNSET` when there are none
    """
    fields = getattr(obj, "_fields", None)
    if not isinstance(fields, tuple) or not fields:
        return _UNSET
    out = {}
    for name in fields[:_MAX_ITEMS]:
        if not isinstance(name, str) or name.lower() in _SKIP_NAMES:
            continue
        if name.lower() in budget.summarise:
            out[name] = _describe(getattr(obj, name, None))
            continue
        if name.lower() in _DATA_NAMES:
            out[name] = _describe_bulk_data(getattr(obj, name, None))
            continue
        try:
            value = getattr(obj, name)
        except Exception:  # pylint: disable=broad-exception-caught
            # A field that needs something the object no longer has.
            out[name] = f"<{name}: unreadable>"
            continue
        out[name] = dump(value, budget=budget, depth=depth)
    return out or _UNSET


def _is_namedtuple(obj: Any) -> bool:
    """
    Tell a namedtuple from a tuple.

    :param obj: the value being walked
    :return: whether it carries field names worth keeping
    """
    return isinstance(obj, tuple) and hasattr(obj, "_fields")


def _has_property(obj: Any, name: str) -> bool:
    """
    Tell whether the object's class exposes `name` as a property.

    :param obj: the object being walked
    :param name: the attribute name
    :return: whether reading it runs the class's own code
    """
    return isinstance(getattr(type(obj), name, None), property)


def _dump_property(obj: Any, name: str, budget: _Budget, depth: int) -> Any:
    """
    Read one property, describing the object if the read raises.

    :param obj: the object being walked
    :param name: the property name
    :param budget: remaining size allowance
    :param depth: depth for the value
    :return: the property's value, dumped
    """
    try:
        value = getattr(obj, name)
    except Exception:  # pylint: disable=broad-exception-caught
        # A property that needs a live session or a lock is the tool's
        # business; the rest of the object is still worth having.
        return f"<{name}: unreadable>"
    return dump(value, budget=budget, depth=depth)


def _is_scalar(obj: Any) -> bool:
    """
    Tell a value that is already JSON from one that has to be walked.

    :param obj: the value being walked
    :return: whether it needs no walking
    """
    return obj is None or isinstance(obj, _SCALARS)


def _is_bulk_data(obj: Any) -> bool:
    """
    Tell a table of the customer's data from metadata about one.

    Array-like rather than by module, so pandas, polars, numpy, pyarrow and
    Spark are all caught without importing any of them, and a pandas
    timestamp or a numpy number is not.

    :param obj: the value being walked
    :return: whether it holds data values
    """
    shape = getattr(obj, "shape", None)
    if isinstance(shape, tuple) and shape:
        return all(isinstance(size, int) for size in shape)
    # A Spark frame has no shape; it does have a schema.
    return hasattr(obj, "columns") and hasattr(obj, "dtypes")


def _describe_bulk_data(obj: Any) -> str:
    """
    Name a table without disclosing a single value of it.

    Not `_describe`: a frame's `__str__` prints its rows.

    :param obj: the frame, series, array or table
    :return: its type and, where it has one, its shape
    """
    name = type(obj).__name__
    shape = getattr(obj, "shape", None)
    if isinstance(shape, tuple) and all(isinstance(size, int) for size in shape):
        return f"<{name} {list(shape)}>"
    return f"<{name}>"


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
    :param depth: the object's own depth; its mapping is walked at it
    :return: the dumped value, or `_UNSET` if no method worked
    """
    for attr in _DUMP_METHODS:
        method = getattr(obj, attr, None)
        if not callable(method):
            continue
        try:
            described = method()
        except Exception:  # pylint: disable=broad-exception-caught
            # A tool's own serialiser failing is not ours to fix, and it is
            # not a reason to stop trying: a Dagster record carries an
            # `_asdict` that raises, and its fields are readable another way.
            continue
        if isinstance(described, dict):
            return _dump_mapping(described, budget, depth + 1)
        return dump(described, budget=budget, depth=depth)
    return _UNSET
