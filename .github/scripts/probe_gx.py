"""Forwards a validation result through whichever action class this GX needs.

The 0.x and 1.x action APIs are incompatible, so this also asserts the
dispatcher picked the right one.
"""

import json
import os

import great_expectations as gx

import convalesce_emit_gx as cegx

major = int(str(gx.__version__).split(".", 1)[0])
assert cegx.GX_MAJOR == major, f"dispatcher chose {cegx.GX_MAJOR} for GX {gx.__version__}"

sent = []


class _Recorder:
    def emit(self, **kwargs):
        sent.append(kwargs)

    def flush(self):
        pass


result = {
    "result": {
        "element_count": 100,
        "unexpected_count": 2,
        "partial_unexpected_list": ["alice@example.com", "bob@example.com"],
    }
}


def forward():
    """Run the action once and return what it put on the wire."""
    del sent[:]
    recorder = _Recorder()
    if major >= 1:
        action = cegx.ConvalesceValidationAction()
        action.set_emitter(recorder)
        outcome = action.run(result, None)
    else:
        action = cegx.ConvalesceValidationAction(emitter=recorder)
        outcome = action._run(result, "ident", "asset", None, None, None)
    assert outcome["convalesce_emitted"], outcome
    return json.dumps(sent, default=str)


# The failing values say which rows broke the check, so they are sent.
os.environ.pop("CONVALESCE_GX_SEND_SAMPLES", None)
wire = forward()
assert "alice@example.com" in wire, "the failing values were dropped"
assert "100" in wire, "the counts detection reads were dropped"

# The handbook promises the switch keeps them back. This is where that stays true.
os.environ["CONVALESCE_GX_SEND_SAMPLES"] = "false"
wire = forward()
assert "alice@example.com" not in wire, "a row value reached the wire"
assert "100" in wire, "the counts detection reads were dropped"

print(f"PROBE_OK gx {gx.__version__} major {major}")
