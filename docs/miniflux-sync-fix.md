# Miniflux Google Reader sync fix

Fix branch: `fix/miniflux-greader-sync`, based on upstream `955498b`.
Upstream issue: <https://github.com/l0ng-ai/papr/issues/127>.
The upstream MIT license and copyright notice remain in `LICENSE`.

## Behavior

Miniflux 2.3.5 implements `GET stream/items/ids` and
`POST stream/items/contents`, rather than FreshRSS's `GET stream/contents`.
The Miniflux provider now enumerates unread and starred references using
continuation offsets, then resolves them in batches of 100 with repeated `i`
form fields. Decimal reference IDs are checked against the hexadecimal item IDs
in the returned content; the content's long-form ID is stored for `edit-tag`.
FreshRSS continues using its existing endpoint.

Both complete sets must be available before read/starred reconciliation starts.
Missing/null array fields, HTTP errors, invalid IDs, duplicate IDs, invalid
continuation offsets, empty pages with continuation, repeated pages, exhausted
page limits, unresolved content IDs and items without URLs abort that sweep.
A valid explicit empty array is an authoritative empty set. Failed or non-`OK`
edit responses requeue local changes; queued articles remain protected from
pull reconciliation.

The safety bounds are 30 ID pages of 1000 references per set and batches of 100
contents. The limit is an error when another page remains, never permission to
apply a partial set. The API has no snapshot token or total-count field: a
server that silently omits data and continuation while returning an otherwise
valid response cannot be distinguished from a complete set. Concurrent server
changes can also affect offset pagination. This patch does not claim stronger
snapshot guarantees than the protocol supplies.

Existing scope is retained: articles are matched by URL; only articles in
server-known feeds are swept. Historical read/unstarred articles without a
remote ID continue waiting for a matching unread/starred pull before queued
edits can be sent. General queue crash recovery and overlapping sync runs are
separate pre-existing concerns.

Protocol references checked directly against Miniflux 2.3.5:

- <https://github.com/miniflux/v2/blob/2.3.5/internal/googlereader/handler.go>
- <https://github.com/miniflux/v2/blob/2.3.5/internal/googlereader/response.go>
- <https://github.com/miniflux/v2/blob/2.3.5/internal/googlereader/item.go>

## Verification

The new tests use only ephemeral localhost HTTP servers and in-memory SQLite.
No production Miniflux requests, Papr database access or real credentials are
needed. They cover ID pagination, 100-item batching, reordered results, decimal
versus hexadecimal ID mapping, all four read/star add/remove edits, server-side
changes, explicit empty sets, local-only feed protection, failed pushes/retries,
malformed/incomplete replies and FreshRSS regression scenarios.

```sh
cargo test --workspace --locked
pnpm build
pnpm test
```

Verified locally on Apple Silicon macOS: 269 Rust tests passed (30 desktop,
13 CLI, 226 core), 76 frontend tests passed, and `pnpm build` succeeded. The
existing upstream CI only runs desktop-package tests. Changing it to
`cargo test --workspace --locked` requires GitHub OAuth `workflow` scope, which
the current login lacks; the separate `ci-workspace-tests.patch` deliverable
contains that change. Run the workspace command above locally until the
workflow patch can be applied. The isolated `.app` was built and its ad-hoc code signature
verified; it has not been launched or connected to production.

One upstream AI test is intentionally ignored because it requires real provider
credentials. Do not enable it for sync validation.

## Isolated macOS build

```sh
pnpm install --frozen-lockfile
pnpm tauri build --debug --bundles app --config src-tauri/tauri.miniflux-test.conf.json
```

The optional config produces `Papr Miniflux Test.app` with its own bundle ID
`com.zmes50416.papr.miniflux-test`, its own macOS data directory
`~/Library/Application Support/com.zmes50416.papr.miniflux-test/`, a separate
URL scheme, and no upstream updater endpoint. It does not migrate or open the
regular Papr database. The local app is ad-hoc signed, not Developer ID signed
or notarized; it is a local test build, not a published release.

Leave the existing Papr installation in place. Start with the isolated app's
empty library and a disposable Miniflux account/server containing synthetic
feeds. Validate remote/local unread and star changes there before connecting
production. Do not copy production credentials or a live database into the
fixture. No production connection or full sync has been performed as part of
this patch.

Papr's upstream desktop targets are macOS, Windows and Linux. This does not
provide an iOS or BOOX Android app; those devices can keep using their existing
Miniflux-compatible clients. The companion CLI still defaults to regular Papr's
data directory: explicitly pass `--db` pointing to the test database when using
it with this isolated app.
