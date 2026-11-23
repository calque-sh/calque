---
kind: changed
---
A page's first catch-up sends each row its transactions change once per
batch, as the batch's last transaction leaves it, instead of every
transaction in turn, so a page opened under steady writes downloads and
applies fewer changes. Change batches are now up to 16 MiB. `calque dev` and
`calque serve` take `--no-compacted-catch-up` to send every transaction as
before.
