# R82 SPEC — Scheduler close diagnostic isolation

Scheduler close diagnostics must never prevent signals, queue, dupefilter, or
manager teardown. Real close control exceptions retain their existing first
error semantics after all cleanup phases are attempted.
