"""External Systems Simulator — FIN, SnF and FCC for the Payment Hub.

Part of the platform deliverable, not a test fixture bolted on: the Hub cannot
be developed, integration-tested or performance-tested without it. It serves
the exact gRPC contract in ``proto/ext/v1`` and calls back into the Hub edge
the way the real networks do, so switching the Hub from the simulator to
production is configuration, not code.

One image, three modes (functional, CI, performance), selected by
``ESS_MODE`` or ``ess serve --mode``.
"""
