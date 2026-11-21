# ``BTree``

`DiskBTree` is an embedded key-value store. Each record has a `(partition, sortKey)` key. You can look up an exact key, scan a partition in sort-key order, and batch inserts, replacements, and deletes in one transaction.

Use ``DiskBTree/create(in:)`` to create a store's files in an existing directory. Use `open(in:)` to reopen a store and check its manifest, key ordering, value schema, page file size, and root page. Reads validate descendant pages as they visit them.

The write closure reads its own changes. `updateValue` and `removeValue` return the previous value, if any.

## Write transactions

The pager syncs the replacement pages and manifest, renames the manifest, then syncs the directory. Fresh readers wait for that final sync. If it fails, the pager restores and syncs the previous manifest before returning an I/O error.

- Scoped closures propagate caller errors. Throwing from a write closure aborts the transaction.
- `recoveryRequired` means the pager could not make the outcome durable after a failed commit or interrupted write. The next access tries to confirm the active manifest before using it.
- A close error after a durable commit is logged. The transaction returns success.

## Read snapshots

The root page is the top of the BTree. The tree reachable from it contains all the records for one version. The manifest names the root page for the currently published version. A snapshot keeps that root page ID and holds the pages file open for the entire closure, so lookups and scan pages see the same version even if a writer publishes a new root. See ``DiskBTree/withReadSnapshot(execute:)``.

`after` is exclusive and `limit` must be positive. For a nonempty page, `nextAfter` is present when another record follows in that snapshot.

Custom keys must use a stable total order, round-trip through `Codable`, and use a distinct, namespaced ``DiskBTreeKey/diskOrderingID``. Change the ID when the persistent encoding or comparison changes.

## Current limitations

- **One writer at a time.** A competing writer gets ``DiskBTreeError/storeInUse``. Existing snapshots continue during publication.
- **Small records.** The encoded composite key is limited to 1,024 bytes. The entire record, including key, value, and framing, is limited to 2,028 bytes.
- **Disk space grows with updates and deletes.** Writes append replacement pages, and reclaiming obsolete pages currently requires a rebuild.

## Topics

### Examples

- <doc:DiskBTreeExample>
