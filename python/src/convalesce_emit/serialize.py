"""
Best-effort JSON shaping of a tool's own objects.

Deliberately shallow and forgiving. This is not modelling anything: it only
gets a tool's object across as JSON so the receiver can interpret it.
Anything unserialisable is stringified rather than dropped, because a partial
observation is worth more than none, and nothing here may raise into the
customer's pipeline.

Envelope version 2: whole-payload forwarding. A repeated subtree -- the same
object reached two ways, such as an Airflow task's DAG reached through the
task and again through the dag run -- is dumped once and pointed at from
everywhere else it occurs, rather than being dumped (or silently truncated)
every time. The first dict-shaped value built for a given object is returned
bare; only if something later points back at it does it get stamped
`{"$id": n, ...}` in place, so a payload with nothing repeated in it pays no
overhead for a mechanism it never needed. A later occurrence of the same
object becomes `{"$ref": n}`, including a true cycle, which resolves to the
`$id` its own ancestor is stamped with once that ancestor finishes.

What used to be silent caps -- on depth, on node count, on list length, on
string length -- are gone as truncation policy. Whole payload, no truncation
is the point of this version. What is left of them are backstops: numbers
high enough that no real tool object should ever reach them (the deepest
thing these tools nest sits at eight; see below), kept only so a genuinely
pathological object graph cannot allocate without bound inside a customer's
worker or blow the Python recursion limit. Hitting one is never silent: it is
recorded in the budget's `excluded` list, by path and reason, the same as the
bulk-data guard and a per-plugin skip. A caller that wants those exclusions
builds a `Budget` with `new_budget()`, passes it to every `dump()` call that
should share it, and reads `budget.excluded` when done.

Import as:

import convalesce_emit.serialize as ceserial
"""

import base64
import dataclasses
import datetime
import decimal
import enum
import io
import logging
import math
import threading
import types
import uuid
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

_LOG = logging.getLogger(__name__)

_SCALARS = (bool, int, float, str)
# Tried in order. Pydantic models answer to the first, Airflow and Dagster
# objects usually to the second or third, Great Expectations results to the
# fourth, and namedtuples to the last.
_DUMP_METHODS = ("model_dump", "dict", "to_dict", "to_json_dict", "_asdict")

# Backstops, not truncation policy. Envelope version 2 forwards the whole
# payload; these exist only to protect the customer's process from a
# genuinely pathological object graph, and hitting one is always recorded in
# `excluded` rather than silently swallowed.
#
# Depth: kept low enough to stay well under Python's default recursion limit
# (1000), since a `dump()` level costs several stack frames. A Great
# Expectations checkpoint result arrives as a keyword argument (2), dumps to
# a mapping of validation results (3, 4), each dumping to a list of
# expectation results (5), each holding an expectation config (6) whose
# kwargs (7) name the column (8) -- eight, on the deepest real path. 150 is
# thirty times that.
_DEPTH_BACKSTOP = 150
# Total dict/object-shaped values dumped. A real DAG event carries one task
# at about sixty values each; this leaves room for tens of thousands of
# tasks.
_NODE_BACKSTOP = 500_000
_STRING_BACKSTOP = 2_000_000

# Names that are always plumbing, wherever they appear (matched bare,
# lower-cased). A dotted, lower-cased path such as "task.dag.parent" matches
# only that one location -- how a plugin excludes a specific known-runtime or
# known-duplicate field without blacklisting a name a payload might also use
# legitimately elsewhere. "parent" and "root" used to be in this default set
# and are not any more, for exactly that reason: a payload key legitimately
# called `parent` or `root` must not vanish just because some other tool's
# object happens to keep a live back-reference under the same name.
DEFAULT_SKIP = frozenset(
    {
        "filters",
        "handlers",
        "lock",
        "log",
        "logger",
        "loggerdict",
        "manager",
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
# Budget
# #############################################################################


@dataclasses.dataclass
class Budget:
    """
    How much of one payload is left to spend, and what has been seen.

    Shared across every `dump()` call that should collapse repeats against
    each other -- one payload's worth, not one call's worth. A plugin that
    dumps a dict's values one key at a time (Airflow's `shape()`, which dumps
    the task instance and the dag run separately) must build one `Budget`
    with `new_budget()` and pass it to every call, or a subtree repeated
    across two keys will not collapse.

    :param nodes: dict/object/list values still allowed before the node
        backstop trips
    :param seen: id(obj) of every dict/object subtree dumped so far, to the
        ref number it was assigned
    :param in_progress: id(obj) of every dict/object subtree currently being
        built, so a true cycle is recognised before it recurses forever
    :param dumped: ref number to the actual dumped dict for it, so a later
        occurrence can stamp `$id` onto it in place
    :param referenced: ref numbers actually pointed at by a `$ref`, so a
        subtree that is never repeated is never stamped
    :param list_active: id(obj) of every list-shaped value currently being
        walked, cycle protection only -- lists are not ref-collapsed, since a
        list cannot be stamped with `$id` after the fact the way a dict can
    :param next_ref: the next ref number to hand out
    :param summarise: field names to name rather than walk into
    :param skip: names and dotted paths to drop entirely
    :param excluded: every path dropped, summarised or capped, and why
    :param keepalive: a strong reference to every object registered in
        `seen`, held for the budget's whole lifetime. `seen` is keyed by
        `id(obj)`, which CPython reuses once `obj` is garbage collected; a
        plugin building an object inline and dumping it in the same
        expression (`dump(Task(dag), ...)`, with nothing else holding the
        `Task`) leaves it eligible for collection the moment `dump()`
        returns, and a later, unrelated object could then land at the same
        address and be mistaken for a repeat. Found by this package's own
        test suite failing intermittently, not by reasoning about it first.
    """

    nodes: int = _NODE_BACKSTOP
    seen: Dict[int, int] = dataclasses.field(default_factory=dict)
    in_progress: Set[int] = dataclasses.field(default_factory=set)
    dumped: Dict[int, Dict[str, Any]] = dataclasses.field(default_factory=dict)
    referenced: Set[int] = dataclasses.field(default_factory=set)
    list_active: Set[int] = dataclasses.field(default_factory=set)
    next_ref: int = 0
    summarise: FrozenSet[str] = frozenset()
    skip: FrozenSet[str] = DEFAULT_SKIP
    excluded: List[Dict[str, str]] = dataclasses.field(default_factory=list)
    keepalive: List[Any] = dataclasses.field(default_factory=list)

    def spend(self) -> bool:
        """
        Take one node from the budget.

        :return: whether there was anything left to take
        """
        self.nodes -= 1
        return self.nodes > 0

    def exclude(self, path: str, reason: str) -> None:
        """
        Record that something at `path` did not cross whole.

        :param path: dotted path of the value affected, empty for the root
        :param reason: short, stable reason a receiver can key off
        :return: nothing
        """
        self.excluded.append({"path": path or "$", "reason": reason})


def new_budget(
    *,
    summarise: FrozenSet[str] = frozenset(),
    skip: Optional[FrozenSet[str]] = None,
) -> Budget:
    """
    Build one budget to share across a payload's `dump()` calls.

    :param summarise: field names to name rather than walk into
    :param skip: names and dotted paths to drop, on top of `DEFAULT_SKIP`
        -- additive, not a replacement, so a plugin naming its own
        known-duplicate paths does not have to re-list the plumbing names
        every caller needs dropped too
    :return: a fresh budget, nothing dumped yet
    """
    extra = frozenset(name.lower() for name in skip) if skip else frozenset()
    return Budget(
        summarise=frozenset(name.lower() for name in summarise),
        skip=DEFAULT_SKIP | extra,
    )


def dump(
    obj: Any,
    *,
    budget: Optional[Budget] = None,
    depth: int = 0,
    summarise: FrozenSet[str] = frozenset(),
    skip: Optional[FrozenSet[str]] = None,
    path: str = "",
) -> Any:
    """
    Convert a tool's object into something JSON can carry.

    The return type is `Any` because both ends of this are boundaries: the
    input came from a tool we do not own, and the output goes to a receiver
    that treats it as opaque.

    :param obj: whatever the tool handed the callback
    :param budget: shared budget, created from `summarise`/`skip` on the
        first call when not given
    :param depth: recursion depth
    :param summarise: fields the tool knows are duplication, named rather
        than walked; a plugin passes what its own tool duplicates. Ignored
        once `budget` is given -- the budget already carries it
    :param skip: names and dotted paths to drop; ignored once `budget` is
        given
    :param path: dotted path of `obj` from the root of this payload, used
        only to say where an exclusion happened
    :return: a JSON-encodable equivalent
    """
    top_level = budget is None
    active: Budget = (
        budget
        if budget is not None
        else new_budget(summarise=summarise, skip=skip)
    )
    try:
        return _dump(obj, active, depth, path)
    except RecursionError:
        # A non-repeating chain of genuinely distinct objects, deep enough to
        # outrun even the depth backstop's stack-safety margin. Every real
        # tool object graph this package has ever seen tops out at eight
        # levels; this is the last-resort net under a shape none of them has.
        if not top_level:
            raise
        active.exclude(path, "recursion backstop")
        return "...(excluded: recursion backstop)"
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # One value that cannot be walked costs that value, not the whole
        # event it sits in: a dict another thread changes mid-iteration, or a
        # value that runs the tool's code merely by being looked at. An
        # Airflow `PythonOperator` taking `**context` keeps the context in
        # `op_kwargs` after it runs, and its lazy `triggering_dataset_events`
        # merges a dag run into a session that refuses it; `isinstance` alone
        # fires that. Found on a live Airflow 2.10 losing every success event
        # of such a task, not by reading. `type()` does not resolve a proxy,
        # so naming it is safe.
        active.exclude(path, f"unreadable: {type(exc).__name__}")
        return f"<{type(obj).__name__}: unreadable>"


def _dump(  # pylint: disable=too-many-return-statements
    obj: Any, budget: Budget, depth: int, path: str
) -> Any:
    """
    Dispatch one value to the right leaf, list or ref handling.

    :param obj: the value being walked
    :param budget: remaining size allowance and what has been seen
    :param depth: recursion depth
    :param path: dotted path of `obj` from the payload root
    :return: a JSON-encodable equivalent
    """
    if obj is None or isinstance(obj, (bool, int)):
        return obj
    if isinstance(obj, float):
        # JSON has no NaN or Infinity. Python writes them anyway and a strict
        # parser refuses the batch, which loses every observation in it.
        return obj if math.isfinite(obj) else None
    if isinstance(obj, str):
        if len(obj) <= _STRING_BACKSTOP:
            return obj
        budget.exclude(path, "string exceeds size backstop")
        return obj[:_STRING_BACKSTOP] + "...(truncated)"
    if isinstance(obj, enum.Enum):
        # The member's value is the tool's state; its name is Python's.
        return _dump(obj.value, budget, depth, path)
    # datetime.datetime is a datetime.date, so this must come first.
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, decimal.Decimal):
        # Not float: a decimal is often an exact value, and float would lose
        # that precision silently.
        return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    if isinstance(obj, _SKIP_TYPES):
        return _describe(obj)
    if _is_bulk_data(obj):
        budget.exclude(path, "bulk data")
        return _describe_bulk_data(obj)
    if isinstance(obj, (list, tuple, set, frozenset)) and not _is_namedtuple(
        obj
    ):
        # Transparent: the items are at the same depth as the list. Not
        # ref-collapsed -- lists cannot be stamped with `$id` after the fact
        # the way a dict can, and a list reached two ways is not the shape
        # issue #34 was about -- but still cycle-guarded, since a list
        # holding itself is exactly as real a risk as an object holding
        # itself.
        return _dump_list(obj, budget, depth, path)
    return _dump_ref(obj, budget, depth, path)


def _dump_list(obj: Any, budget: Budget, depth: int, path: str) -> Any:
    """
    Walk a list-shaped value, guarding only against it containing itself.

    :param obj: the list, tuple, set or frozenset being walked
    :param budget: remaining size allowance
    :param depth: recursion depth, unchanged for the items
    :param path: dotted path of `obj` from the payload root
    :return: a JSON list
    """
    marker = id(obj)
    if marker in budget.list_active:
        return "...(cycle)"
    if not budget.spend():
        budget.exclude(path, "node backstop")
        return "...(truncated: size limit)"
    budget.list_active.add(marker)
    try:
        return [
            dump(v, budget=budget, depth=depth, path=f"{path}[{i}]")
            for i, v in enumerate(obj)
        ]
    finally:
        budget.list_active.discard(marker)


def _dump_ref(obj: Any, budget: Budget, depth: int, path: str) -> Any:
    """
    Walk a dict- or object-shaped value, collapsing a repeat into `$ref`.

    The first occurrence is dumped in full and registered under an integer
    id. A later occurrence -- reached through a different path, or the same
    object referencing itself -- becomes `{"$ref": n}` instead of being
    walked again. The occurrence being pointed at is stamped `{"$id": n,
    ...}` in place only once something actually points at it, so a subtree
    that never repeats carries no overhead for a mechanism it never needed.

    :param obj: the dict, namedtuple or other object being walked
    :param budget: remaining size allowance and what has been seen
    :param depth: recursion depth
    :param path: dotted path of `obj` from the payload root
    :return: a JSON-encodable equivalent, or a `$ref`/`$id` pointer
    """
    existing_ref = budget.seen.get(id(obj))
    if existing_ref is not None:
        if id(obj) in budget.in_progress:
            # A true cycle: the ancestor that owns this ref is still being
            # built further up the call stack. It will be stamped once that
            # frame returns, because `existing_ref` is now in `referenced`.
            budget.referenced.add(existing_ref)
            return {"$ref": existing_ref}
        target = budget.dumped.get(existing_ref)
        if target is not None:
            target["$id"] = existing_ref
            budget.referenced.add(existing_ref)
            return {"$ref": existing_ref}
        # The first dump of this object was not dict-shaped (its own dump
        # method returned something else, or it had nothing to describe
        # itself with beyond a repr), so there is nothing to point at. Rare
        # enough, and cheap enough when it happens, to just redo the work
        # rather than carry a second cache for it.
        return _dump_container(obj, budget, depth, path)
    if not budget.spend():
        budget.exclude(path, "node backstop")
        return "...(truncated: size limit)"
    if depth >= _DEPTH_BACKSTOP:
        budget.exclude(path, "depth backstop")
        return _describe(obj)
    ref = budget.next_ref
    budget.next_ref += 1
    budget.seen[id(obj)] = ref
    # Keep `obj` alive for as long as `seen` remembers its id -- see the
    # docstring on `Budget.keepalive`.
    budget.keepalive.append(obj)
    budget.in_progress.add(id(obj))
    try:
        value = _dump_container(obj, budget, depth, path)
    finally:
        budget.in_progress.discard(id(obj))
    if isinstance(value, dict):
        budget.dumped[ref] = value
        if ref in budget.referenced:
            value["$id"] = ref
    return value


def _dump_container(obj: Any, budget: Budget, depth: int, path: str) -> Any:
    """
    Walk one level of a container or object.

    :param obj: the value being walked
    :param budget: remaining size allowance
    :param depth: current recursion depth
    :param path: dotted path of `obj` from the payload root
    :return: a JSON-encodable equivalent
    """
    nxt = depth + 1
    if isinstance(obj, dict):
        return _dump_mapping(obj, budget, nxt, path)
    # An object and the mapping it dumps to are one level, not two: the
    # mapping is the object, described by the tool's own method.
    dumped = _try_dump_methods(obj, budget, depth, path)
    if dumped is not _UNSET:
        return dumped
    fields = _dump_fields(obj, budget, nxt, path)
    if fields is not _UNSET:
        return fields
    data = _probe(obj, "__dict__")
    if isinstance(data, dict):
        public = {}
        for key, value in data.items():
            name = str(key)
            child_path = f"{path}.{name}" if path else name
            if _is_skipped(name, path, budget.skip):
                budget.exclude(child_path, "excluded by name")
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
                    and not _is_skipped(exposed, path, budget.skip)
                    and _has_property(obj, exposed)
                ):
                    exposed_path = f"{path}.{exposed}" if path else exposed
                    public[exposed] = _dump_property(
                        obj, exposed, budget, nxt, exposed_path
                    )
                continue
            if name.lower() in budget.summarise and not _is_scalar(value):
                public[name] = _describe(value)
                continue
            if name.lower() in _DATA_NAMES and not _is_scalar(value):
                budget.exclude(child_path, "bulk data")
                public[name] = _describe_bulk_data(value)
                continue
            public[name] = dump(value, budget=budget, depth=nxt, path=child_path)
        # An empty mapping tells the receiver nothing; the object's own
        # description at least names what it was.
        if public:
            return public
    return _describe(obj)


def _dump_mapping(
    obj: Dict[Any, Any], budget: Budget, depth: int, path: str
) -> Any:
    """
    Walk one mapping.

    :param obj: the mapping being walked
    :param budget: remaining size allowance
    :param depth: depth for the values
    :param path: dotted path of `obj` from the payload root
    :return: a JSON-encodable mapping keyed by strings
    """
    out = {}
    for key, value in obj.items():
        # Keys are whatever the tool used; JSON wants strings. A Great
        # Expectations checkpoint keys its results by identifier objects.
        name = key if isinstance(key, str) else _describe(key)
        child_path = f"{path}.{name}" if path else name
        if _is_skipped(name, path, budget.skip):
            budget.exclude(child_path, "excluded by name")
            continue
        if name.lower() in budget.summarise and not _is_scalar(value):
            out[name] = _describe(value)
            continue
        if name.lower() in _DATA_NAMES and not _is_scalar(value):
            budget.exclude(child_path, "bulk data")
            out[name] = _describe_bulk_data(value)
            continue
        out[name] = dump(value, budget=budget, depth=depth, path=child_path)
    return out


def _dump_fields(obj: Any, budget: Budget, depth: int, path: str) -> Any:
    """
    Read an object that declares its own field names.

    Namedtuples and Dagster's `@record` classes both carry `_fields`, and a
    record keeps nothing in `__dict__`: a Dagster job snapshot walked by
    attribute is empty, and its `_asdict` raises "Iteration is not allowed",
    so without this the whole snapshot arrives as its repr.

    Airflow 3.2's `on_asset_event_emitted` hands the listener an
    `attrs.define`-decorated `AssetEvent`, and `attrs.define` defaults to
    `slots=True`: no `__dict__` either, and no `_fields` -- attrs' own
    marker is `__attrs_attrs__`, on the class rather than the instance.
    Found by dumping a real one and getting its repr back instead of a
    payload, not by reading attrs' docs first.

    :param obj: the object being walked
    :param budget: remaining size allowance
    :param depth: depth for the values
    :param path: dotted path of `obj` from the payload root
    :return: the fields as a mapping, or `_UNSET` when there are none
    """
    fields = _probe(obj, "_fields")
    if not isinstance(fields, tuple) or not fields:
        fields = _attrs_field_names(obj)
    if not isinstance(fields, tuple) or not fields:
        return _UNSET
    out = {}
    for name in fields:
        if not isinstance(name, str):
            continue
        child_path = f"{path}.{name}" if path else name
        if _is_skipped(name, path, budget.skip):
            budget.exclude(child_path, "excluded by name")
            continue
        if name.lower() in budget.summarise:
            out[name] = _describe(_probe(obj, name))
            continue
        if name.lower() in _DATA_NAMES:
            budget.exclude(child_path, "bulk data")
            out[name] = _describe_bulk_data(_probe(obj, name))
            continue
        try:
            value = getattr(obj, name)
        except Exception:  # pylint: disable=broad-exception-caught
            # A field that needs something the object no longer has.
            out[name] = f"<{name}: unreadable>"
            continue
        out[name] = dump(value, budget=budget, depth=depth, path=child_path)
    return out or _UNSET


def _attrs_field_names(obj: Any) -> Optional[Tuple[str, ...]]:
    """
    Field names for an `attrs`-decorated object, without depending on attrs.

    `__attrs_attrs__` is a tuple of `Attribute` objects, each carrying the
    field's name; reading it by attribute means this works whether or not
    the `attrs` package itself is importable in this process.

    :param obj: the object being walked
    :return: the field names, or None when the object is not attrs-decorated
    """
    declared = _probe(type(obj), "__attrs_attrs__")
    if not isinstance(declared, tuple) or not declared:
        return None
    names: List[str] = []
    for field in declared:
        name = _probe(field, "name")
        if not isinstance(name, str):
            return None
        names.append(name)
    return tuple(names)


def _is_skipped(name: str, path: str, skip: FrozenSet[str]) -> bool:
    """
    Tell whether `name` at `path` is one of the caller's skip entries.

    A bare, lower-cased name matches wherever it occurs, the way the old
    global `_SKIP_NAMES` did. A dotted, lower-cased path matches only that
    exact location, which is how a caller excludes one specific known-runtime
    or known-duplicate field without blacklisting a name the payload might
    also use legitimately somewhere else.

    :param name: the attribute or key name being considered
    :param path: dotted path of the value the name is on, not including name
    :param skip: names and dotted paths to drop
    :return: whether this occurrence should be dropped
    """
    lowered = name.lower()
    if lowered in skip:
        return True
    full = f"{path}.{lowered}" if path else lowered
    return full in skip


def _is_namedtuple(obj: Any) -> bool:
    """
    Tell a namedtuple from a tuple.

    :param obj: the value being walked
    :return: whether it carries field names worth keeping
    """
    return isinstance(obj, tuple) and _probe(obj, "_fields") is not None


def _has_property(obj: Any, name: str) -> bool:
    """
    Tell whether the object's class exposes `name` as a property.

    :param obj: the object being walked
    :param name: the attribute name
    :return: whether reading it runs the class's own code
    """
    return isinstance(_probe(type(obj), name), property)


def _dump_property(
    obj: Any, name: str, budget: Budget, depth: int, path: str
) -> Any:
    """
    Read one property, describing the object if the read raises.

    :param obj: the object being walked
    :param name: the property name
    :param budget: remaining size allowance
    :param depth: depth for the value
    :param path: dotted path the property's value is at
    :return: the property's value, dumped
    """
    try:
        value = getattr(obj, name)
    except Exception:  # pylint: disable=broad-exception-caught
        # A property that needs a live session or a lock is the tool's
        # business; the rest of the object is still worth having.
        return f"<{name}: unreadable>"
    return dump(value, budget=budget, depth=depth, path=path)


def _probe(obj: Any, name: str) -> Any:
    """
    Read an attribute we are only checking for, treating any failure as absent.

    `getattr(obj, name, None)` only swallows AttributeError. An object whose
    `__getattr__` looks the name up elsewhere raises something else for a name
    it does not have: Airflow's `var.value` accessor raises KeyError ("Variable
    shape does not exist"), and that lost every failed task's event.

    :param obj: the value being walked
    :param name: the attribute to look for
    :return: its value, or None when reading it fails in any way
    """
    try:
        return getattr(obj, name, None)
    except Exception:  # pylint: disable=broad-exception-caught
        return None


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
    shape = _probe(obj, "shape")
    if isinstance(shape, tuple) and shape:
        return all(isinstance(size, int) for size in shape)
    # A Spark frame has no shape; it does have a schema.
    return (
        _probe(obj, "columns") is not None and _probe(obj, "dtypes") is not None
    )


def _describe_bulk_data(obj: Any) -> str:
    """
    Name a table without disclosing a single value of it.

    Not `_describe`: a frame's `__str__` prints its rows.

    :param obj: the frame, series, array or table
    :return: its type and, where it has one, its shape
    """
    name = type(obj).__name__
    shape = _probe(obj, "shape")
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
    if len(text) > _STRING_BACKSTOP:
        text = text[:_STRING_BACKSTOP] + "...(truncated)"
    return text


# Sentinel: None is a legitimate result of a tool's own dump method.
_UNSET = object()


def _try_dump_methods(obj: Any, budget: Budget, depth: int, path: str) -> Any:
    """
    Ask the object to describe itself.

    :param obj: the object to try
    :param budget: remaining size allowance
    :param depth: the object's own depth; its mapping is walked at it
    :param path: dotted path of `obj` from the payload root
    :return: the dumped value, or `_UNSET` if no method worked
    """
    for attr in _DUMP_METHODS:
        method = _probe(obj, attr)
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
            return _dump_mapping(described, budget, depth + 1, path)
        return dump(described, budget=budget, depth=depth, path=path)
    return _UNSET
