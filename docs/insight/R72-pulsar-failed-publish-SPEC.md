# R72 SPEC — Pulsar failed-publish rollback

Every failed Pulsar connection attempt must leave no generation it owns
published. Post-publication failures must detach only the matching client and
generation, best-effort retire its handles, and preserve the original failure
over cleanup control exceptions.
