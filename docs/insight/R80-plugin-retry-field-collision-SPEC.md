# R80 SPEC — Plugin retry field ownership

Third-party backend settings fields named `retry_attempts` and `retry_delay`
remain public plugin configuration. Only explicit `manager_retry_*` aliases or
global controls configure ConnectionManager when those names collide.
