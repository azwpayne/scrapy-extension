# R81 SPEC — Spider close diagnostic isolation

Diagnostic logging of ordinary component close failures must not interrupt
later SpiderMixin component or manager teardown when a logger handler raises a
control exception.
