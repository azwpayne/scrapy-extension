# R51 SPEC — Release artifact installation smoke

CI builds both wheel and sdist but only checks wheel metadata with
`--no-deps`; it never imports package code and never installs the sdist.
Each artifact must instead install with dependencies into its own venv and
perform an isolated (`-I`) import plus distribution-version check.
