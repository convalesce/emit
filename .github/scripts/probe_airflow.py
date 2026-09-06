"""Registers the listener with a real Airflow and fires a hook through pluggy.

Runs in CI across every Airflow whose listener API differs. Nothing here is
a mock: pluggy rejecting a plugin, or silently passing None for an argument
we declared wrong, is exactly what a fake would hide.
"""

import sys

import convalesce_emit_airflow.listener as cealist
from airflow.listeners.listener import ListenerManager

specs = cealist.read_specs()
assert specs, "no hookspecs could be read from this Airflow"
print("specs:", sorted(specs))

sent = []


class _Recorder:
    def emit(self, **kwargs):
        sent.append(kwargs)

    def flush(self):
        pass


manager = ListenerManager()
listener = cealist.build_listener_class()()
listener._emitter = _Recorder()
manager.add_listener(listener)

# Declaring a hook this Airflow does not specify makes pluggy reject the
# plugin at registration, which would take a customer's scheduler down.
for name, argnames in specs.items():
    hook = getattr(manager.hook, name, None)
    if hook is None:
        continue
    hook(**{arg: "probe-" + arg for arg in argnames})

assert sent, "the listener registered but never fired"
events = sorted({s["event"] for s in sent})
print("fired:", events)

# The failure message is the field detection exists to read. Airflow only
# passes it from 2.10 onwards, so this is conditional on the spec.
failed = [s for s in sent if s["event"] == "on_task_instance_failed"]
if failed and "error" in specs.get("on_task_instance_failed", ()):
    assert failed[0]["payload"].get("error"), "error was declared but arrived empty"
    print("error forwarded:", failed[0]["payload"]["error"])

print("PROBE_OK airflow")
sys.exit(0)
