"""
`python -m convalesce_emit`, for a machine whose scripts directory is not on
PATH.
"""

import sys

import convalesce_emit.cli as cecli

sys.exit(cecli.main())
