"""The test suite.

This runs before anything from the project is imported, which is the only
moment the gRPC deadlines can be scaled: they are module constants.
"""

import os

# Tests make real gRPC calls over loopback. With the production deadlines
# (200 ms to 1 s) a loaded laptop or a shared CI runner fails them at random;
# what the tests check is behaviour, not this machine's speed.
os.environ.setdefault("HUB_DEADLINE_SCALE", "10")
