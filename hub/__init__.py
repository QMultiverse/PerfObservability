"""The Payment Hub services.

One package per stage, matching the pipeline in docs/platform-design.md:
``edge`` (the only external boundary), ``fin_parser`` / ``mx_parser``,
``screening``, ``routing``, ``settlement``, ``dispatcher``, ``ack_matcher``,
``status_api`` and ``db_sink``, with the shared machinery in ``common``.
"""
