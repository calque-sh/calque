---
kind: changed
---
Results with text columns read faster: in Chromium, a page reads a
21,000-row, 46-column result through `window()` in about a third of the time
it took before, and every `run()` and `window()` that returns text gains with
it.
