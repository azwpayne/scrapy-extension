# R67 SPEC — Scheduler partial signal-registration ownership

Every attempted scheduler ack/nack signal registration must be owned by the
scheduler before the signal manager call. If registration or its rollback
fails, terminal scheduler cleanup must retain and retry the owned handlers,
complete all other teardown phases, and preserve the original registration
failure.
