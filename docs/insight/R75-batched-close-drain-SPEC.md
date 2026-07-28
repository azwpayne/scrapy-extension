# R75 SPEC — Batched storage close drain

Closing batched storage must attempt a final serialized drain after any
age-flusher join control exception and wait through the close deadline for any
active flush holder. It then re-raises the first control failure only after the
drain attempt.
