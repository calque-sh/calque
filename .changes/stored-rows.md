---
kind: added
---
A page keeps its working set between visits, encrypted in the browser's
origin private file system, and a returning page reads it from disk instead
of downloading it again. `createSyncClient({ storeRows: false })` keeps
nothing, `SyncClient.clearStoredRows()` erases what is kept, and
`calque serve --no-stored-rows` or `calque dev --no-stored-rows` turns it
off for every page.
