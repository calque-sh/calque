---
kind: fixed
---
A tenant whose rows keep moving to or from other tenants is no longer built
again past its bound on moves while a page waits on it. The page is told the
artifact is preparing, and the tenant is built when a page opens it again.
