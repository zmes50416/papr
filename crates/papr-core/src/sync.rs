//! Synchronisation over the Google Reader compatible API.
//!
//! Supports any GReader-compatible backend; today FreshRSS and Miniflux.
//! Authentication and edits use the same protocol; stream retrieval differs.
//! Miniflux requires `stream/items/ids` followed by `stream/items/contents`,
//! while FreshRSS supports `stream/contents/...`.
//!
//! Flow: `ClientLogin` for an auth token, push any queued local read/starred
//! changes via `edit-tag`, pull the subscription list (to subscribe locally to
//! new server feeds) and push any local-only feeds back to the server (so the
//! two subscription lists converge rather than drifting), then pull the recent
//! reading-list (to reconcile read/starred state, matched to local articles by
//! URL).

use crate::db;
use crate::error::{AppError, AppResult};
use crate::ingestion::parse;
use reqwest::{Client, RequestBuilder};
use rusqlite::Connection;
use serde::Deserialize;
use tokio::sync::Mutex;

/// The writer connection behind an async mutex — what every sync function reads
/// and writes through. The desktop app passes `state.db`; the CLI passes the
/// connection it opened. Paired with a shared [`Client`] for the HTTP calls.
type Db = Mutex<Connection>;

const READ_TAG: &str = "user/-/state/com.google/read";
const STARRED_TAG: &str = "user/-/state/com.google/starred";
const READING_LIST: &str = "user/-/state/com.google/reading-list";

/// Which GReader-compatible backend the user is connected to. This selects
/// both the API root and the supported stream retrieval protocol.
#[derive(Clone, Copy)]
enum Provider {
    FreshRss,
    Miniflux,
}

impl Provider {
    /// Path segment to append to the user-supplied server URL to reach the
    /// GReader API root. Miniflux serves `/accounts/ClientLogin` and
    /// `/reader/api/0/...` straight off the server root, so its suffix is
    /// empty.
    fn path_suffix(self) -> &'static str {
        match self {
            Provider::FreshRss => "/api/greader.php",
            Provider::Miniflux => "",
        }
    }

    /// Parse the persisted setting. Missing / unknown → FreshRss, so older
    /// installs (where this setting didn't exist) keep working unchanged.
    fn from_setting(s: Option<&str>) -> Self {
        match s.unwrap_or("").trim() {
            "miniflux" => Provider::Miniflux,
            _ => Provider::FreshRss,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Provider::FreshRss => "freshrss",
            Provider::Miniflux => "miniflux",
        }
    }
}

/// Normalise a user-supplied server URL to its GReader API root for the
/// chosen provider. Idempotent: if the user already typed the full path,
/// don't append it again.
fn greader_base(url: &str, provider: Provider) -> String {
    let t = url.trim().trim_end_matches('/');
    let suffix = provider.path_suffix();
    if t.ends_with(suffix) || t.contains(&format!("{suffix}/")) {
        t.to_string()
    } else {
        format!("{t}{suffix}")
    }
}

/// An authenticated FreshRSS session.
struct Session {
    base: String,
    auth: String,
    token: String,
}

impl Session {
    fn get(&self, http: &Client, path: &str) -> RequestBuilder {
        http.get(format!("{}/reader/api/0/{path}", self.base))
            .header("Authorization", format!("GoogleLogin auth={}", self.auth))
    }
    fn post(&self, http: &Client, path: &str) -> RequestBuilder {
        http.post(format!("{}/reader/api/0/{path}", self.base))
            .header("Authorization", format!("GoogleLogin auth={}", self.auth))
    }
}

/// Exchange username + password for a long-lived auth token.
async fn client_login(http: &Client, base: &str, user: &str, pass: &str) -> AppResult<String> {
    let resp = http
        .post(format!("{base}/accounts/ClientLogin"))
        .form(&[("Email", user), ("Passwd", pass)])
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(AppError::code("freshrssLoginFailed"));
    }
    let body = resp.text().await?;
    body.lines()
        .find_map(|l| l.strip_prefix("Auth="))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::code("freshrssNoToken"))
}

/// Build a session from an existing auth token by fetching a fresh write
/// (edit-tag) token. Fails fast if the auth token is no longer valid.
async fn session_with_token(http: &Client, base: &str, auth: String) -> AppResult<Session> {
    let token = http
        .get(format!("{base}/reader/api/0/token"))
        .header("Authorization", format!("GoogleLogin auth={auth}"))
        .send()
        .await?
        .error_for_status()
        .map_err(|_| AppError::code("freshrssLoginFailed"))?
        .text()
        .await?
        .trim()
        .to_string();
    Ok(Session {
        base: base.to_string(),
        auth,
        token,
    })
}

/// Log in with username + password and obtain a full session.
async fn login(http: &Client, base: &str, user: &str, pass: &str) -> AppResult<Session> {
    let auth = client_login(http, base, user, pass).await?;
    session_with_token(http, base, auth).await
}

#[derive(Deserialize)]
struct SubList {
    subscriptions: Vec<Sub>,
}
#[derive(Deserialize)]
struct Sub {
    url: Option<String>,
    title: Option<String>,
    #[serde(default)]
    categories: Vec<SubCat>,
}
/// A GReader category ("label") a subscription belongs to. FreshRSS/Miniflux
/// folders surface here; we map the first named one onto a local folder.
#[derive(Deserialize)]
struct SubCat {
    #[serde(default)]
    id: String,
    #[serde(default)]
    label: Option<String>,
}
impl SubCat {
    /// Human folder name for this category. Prefer the explicit `label`,
    /// otherwise derive it from the `user/-/label/NAME` id. `None` for an
    /// unnamed category, so it is skipped rather than creating a blank folder.
    ///
    /// FreshRSS files every feed the user hasn't categorised under a built-in
    /// "Uncategorized" label. That isn't a real folder — mapping it onto a
    /// local one buries every top-level feed in a junk folder that doesn't
    /// match the server's own presentation — so it is treated as no folder.
    fn folder_name(&self) -> Option<String> {
        self.label
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                self.id
                    .rsplit_once("/label/")
                    .map(|(_, n)| n.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .filter(|n| !n.eq_ignore_ascii_case("Uncategorized"))
    }
}

#[derive(Deserialize)]
struct Contents {
    items: Vec<Item>,
    // Present when the stream has more pages; fed back as `c=` to page on.
    #[serde(default)]
    continuation: Option<String>,
}
#[derive(Deserialize)]
struct Item {
    id: String,
    #[serde(default)]
    canonical: Vec<Href>,
    #[serde(default)]
    alternate: Vec<Href>,
}
#[derive(Deserialize)]
struct Href {
    href: String,
}

/// Stored GReader connection. We persist the long-lived auth token rather
/// than the password — a leaked token is revocable server-side and can't be
/// replayed against the user's other accounts. `legacy_pass` holds a
/// plaintext password from an older install, awaiting one-time migration.
struct Creds {
    url: String,
    user: String,
    auth: Option<String>,
    legacy_pass: Option<String>,
    provider: Provider,
}

/// Stored GReader credentials, if a server is configured. The setting keys
/// are still named `freshrss_*` for backwards compatibility with installs
/// that predate multi-provider support — the values are provider-agnostic.
async fn creds(db: &Db) -> AppResult<Option<Creds>> {
    let conn = db.lock().await;
    let url = db::get_setting(&conn, "freshrss_url")?.unwrap_or_default();
    let user = db::get_setting(&conn, "freshrss_user")?.unwrap_or_default();
    let nonempty = |k| db::get_setting(&conn, k).map(|v| v.filter(|s| !s.is_empty()));
    let auth = nonempty("freshrss_auth")?;
    let legacy_pass = nonempty("freshrss_pass")?;
    let provider = Provider::from_setting(db::get_setting(&conn, "freshrss_provider")?.as_deref());
    if url.trim().is_empty() || user.is_empty() || (auth.is_none() && legacy_pass.is_none()) {
        return Ok(None);
    }
    Ok(Some(Creds {
        url,
        user,
        auth,
        legacy_pass,
        provider,
    }))
}

/// The configured GReader server URL and provider, or `None` when not
/// connected.
pub async fn connected_url(db: &Db) -> AppResult<Option<(String, String)>> {
    Ok(creds(db)
        .await?
        .map(|c| (c.url, c.provider.as_str().to_string())))
}

/// Persist a verified connection, storing the auth token and never the
/// password (any legacy stored password is also cleared).
async fn persist_session(
    db: &Db,
    url: &str,
    user: &str,
    auth: &str,
    provider: Provider,
) -> AppResult<()> {
    let conn = db.lock().await;
    db::set_setting(&conn, "freshrss_url", url.trim())?;
    db::set_setting(&conn, "freshrss_user", user)?;
    db::set_setting(&conn, "freshrss_auth", auth)?;
    db::set_setting(&conn, "freshrss_pass", "")?;
    db::set_setting(&conn, "freshrss_provider", provider.as_str())?;
    Ok(())
}

/// Verify credentials against the server and, on success, persist them.
pub async fn connect(
    db: &Db,
    http: &Client,
    url: &str,
    user: &str,
    pass: &str,
    provider: Option<&str>,
) -> AppResult<()> {
    let provider = Provider::from_setting(provider);
    let base = greader_base(url, provider);
    let session = login(http, &base, user, pass).await?; // verifies credentials
    persist_session(db, url, user, &session.auth, provider).await
}

/// Forget the stored GReader credentials.
pub async fn disconnect(db: &Db) -> AppResult<()> {
    let conn = db.lock().await;
    for key in [
        "freshrss_url",
        "freshrss_user",
        "freshrss_auth",
        "freshrss_pass",
        "freshrss_provider",
    ] {
        db::set_setting(&conn, key, "")?;
    }
    Ok(())
}

/// Run a full sync if a server is connected. Returns `true` when a sync
/// actually ran, so the caller can refresh the UI for the reconciled state.
pub async fn run_if_connected(db: &Db, http: &Client) -> AppResult<bool> {
    if creds(db).await?.is_some() {
        sync_now(db, http).await.map(|_| true)
    } else {
        Ok(false)
    }
}

/// Local feed URLs the server doesn't already carry, so each can be subscribed
/// remotely. Pure set difference, factored out of `sync_now` so the selection
/// is unit-testable without a live server.
fn feeds_to_push<'a>(
    local: &'a [String],
    server: &std::collections::HashSet<String>,
) -> Vec<&'a str> {
    local
        .iter()
        .filter(|u| !server.contains(*u))
        .map(String::as_str)
        .collect()
}

/// A single article the server flags as unread or starred: its item id and the
/// canonical URL we match local articles on.
struct RemoteItem {
    id: String,
    url: String,
}

// Limits bound network work, never the authoritative set: hitting a limit
// with more pages is an error, so no partial result reaches reconciliation.
const STREAM_PAGE_SIZE: usize = 1000;
const MAX_STREAM_PAGES: usize = 30;
const CONTENT_BATCH_SIZE: usize = 100;

fn incomplete_stream() -> AppError {
    AppError::other(
        "Sync response was invalid or incomplete; local read/starred state was preserved",
    )
}

/// Serde also accepts positional arrays for derived structs. Require an
/// object at the HTTP boundary so `[[]]` cannot masquerade as an empty stream.
async fn sync_json<T: serde::de::DeserializeOwned>(request: RequestBuilder) -> AppResult<T> {
    let value: serde_json::Value = request.send().await?.error_for_status()?.json().await?;
    if !value.is_object() {
        return Err(incomplete_stream());
    }
    serde_json::from_value(value).map_err(|_| incomplete_stream())
}

impl TryFrom<Item> for RemoteItem {
    type Error = AppError;

    fn try_from(item: Item) -> AppResult<Self> {
        let url = item
            .canonical
            .iter()
            .chain(item.alternate.iter())
            .map(|h| h.href.as_str())
            .find(|url| !url.trim().is_empty())
            .ok_or_else(incomplete_stream)?;
        if item.id.trim().is_empty() {
            return Err(incomplete_stream());
        }
        Ok(Self {
            id: item.id,
            url: url.to_string(),
        })
    }
}

#[derive(Deserialize)]
struct ItemRefs {
    // Required: `{}`, `[]` and null are not an authoritative empty set.
    #[serde(rename = "itemRefs")]
    items: Vec<ItemRef>,
    continuation: Option<String>,
}

#[derive(Deserialize)]
struct ItemRef {
    id: String,
}

/// Miniflux lists decimal IDs but returns prefixed hexadecimal content IDs.
fn miniflux_content_id(id: &str) -> AppResult<u64> {
    let id = id
        .strip_prefix("tag:google.com,2005:reader/item/")
        .ok_or_else(incomplete_stream)?;
    u64::from_str_radix(id, 16)
        .ok()
        .filter(|id| *id > 0 && *id <= i64::MAX as u64)
        .ok_or_else(incomplete_stream)
}

async fn fetch_miniflux_ids(
    session: &Session,
    http: &Client,
    stream: &str,
    xt: Option<&str>,
) -> AppResult<Vec<u64>> {
    let mut ids = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut continuation: Option<String> = None;
    for page_index in 0..MAX_STREAM_PAGES {
        let mut params = vec![
            ("output", "json".to_string()),
            ("s", stream.to_string()),
            ("n", STREAM_PAGE_SIZE.to_string()),
        ];
        if let Some(xt) = xt {
            params.push(("xt", xt.to_string()));
        }
        if let Some(c) = &continuation {
            params.push(("c", c.clone()));
        }
        let page: ItemRefs =
            sync_json(session.get(http, "stream/items/ids").query(&params)).await?;
        if page.items.len() > STREAM_PAGE_SIZE {
            return Err(incomplete_stream());
        }
        let empty = page.items.is_empty();
        for item in page.items {
            let id = item
                .id
                .parse::<u64>()
                .ok()
                .filter(|id| *id > 0 && *id <= i64::MAX as u64)
                .ok_or_else(incomplete_stream)?;
            if !seen.insert(id) {
                return Err(incomplete_stream());
            }
            ids.push(id);
        }
        match page.continuation {
            None => break,
            Some(c) => {
                // Miniflux's continuation is the next offset, not an opaque
                // token. Reject skipped pages, repeated pages and stalled scans.
                if empty
                    || c.parse::<usize>().ok() != Some(ids.len())
                    || page_index + 1 == MAX_STREAM_PAGES
                {
                    return Err(incomplete_stream());
                }
                continuation = Some(c);
            }
        }
    }

    Ok(ids)
}

async fn fetch_miniflux_items(
    session: &Session,
    http: &Client,
    stream: &str,
    xt: Option<&str>,
) -> AppResult<Vec<RemoteItem>> {
    use std::collections::HashSet;
    let ids = fetch_miniflux_ids(session, http, stream, xt).await?;
    let mut out = Vec::with_capacity(ids.len());
    for batch in ids.chunks(CONTENT_BATCH_SIZE) {
        let mut params = vec![("output", "json".to_string()), ("T", session.token.clone())];
        // Miniflux interprets bare 16-character IDs as hex even when the IDs
        // endpoint emitted decimal. Always send the unambiguous long form.
        params.extend(
            batch
                .iter()
                .map(|id| ("i", format!("tag:google.com,2005:reader/item/{id:016x}"))),
        );
        let page: Contents =
            sync_json(session.post(http, "stream/items/contents").form(&params)).await?;
        // Every requested reference must resolve exactly once. A deleted item,
        // capped batch or malformed result requires a retry of the whole sync.
        if page.continuation.is_some() || page.items.len() != batch.len() {
            return Err(incomplete_stream());
        }
        let mut expected: HashSet<u64> = batch.iter().copied().collect();
        for item in page.items {
            if !expected.remove(&miniflux_content_id(&item.id)?) {
                return Err(incomplete_stream());
            }
            out.push(item.try_into()?);
        }
    }
    let confirmed = fetch_miniflux_ids(session, http, stream, xt).await?;
    if ids.iter().copied().collect::<HashSet<_>>() != confirmed.into_iter().collect::<HashSet<_>>()
    {
        return Err(incomplete_stream());
    }
    Ok(out)
}

/// Resolve pending local edits that predate their first unread/starred match.
/// This maps IDs for existing local articles only; it does not import history.
async fn resolve_pending_miniflux_ids(
    db: &Db,
    session: &Session,
    http: &Client,
) -> AppResult<usize> {
    let pending_urls: std::collections::HashSet<String> = {
        let conn = db.lock().await;
        let mut statement = conn.prepare(
            "SELECT DISTINCT a.url FROM sync_queue q JOIN articles a ON a.id=q.article_id
             WHERE a.remote_id IS NULL AND a.url IS NOT NULL",
        )?;
        let urls = statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        urls
    };
    if pending_urls.is_empty() {
        return Ok(0);
    }
    let items = fetch_miniflux_items(session, http, READING_LIST, None).await?;
    let conn = db.lock().await;
    let mut mapped = 0;
    for item in items {
        if pending_urls.contains(&item.url) {
            if let Some(aid) = db::article_id_by_url(&conn, &item.url)? {
                db::set_remote_id(&conn, aid, &item.id)?;
                mapped += 1;
            }
        }
    }
    Ok(mapped)
}

/// Fetch a complete unread/starred set, or fail before the destructive sweep.
async fn fetch_stream_items(
    session: &Session,
    http: &Client,
    provider: Provider,
    stream: &str,
    xt: Option<&str>,
) -> AppResult<Vec<RemoteItem>> {
    if matches!(provider, Provider::Miniflux) {
        return fetch_miniflux_items(session, http, stream, xt).await;
    }
    let path = format!("stream/contents/{stream}");
    let mut out = Vec::new();
    let mut cont: Option<String> = None;
    let mut seen_tokens = std::collections::HashSet::new();
    let mut seen_ids = std::collections::HashSet::new();
    for page_index in 0..MAX_STREAM_PAGES {
        let mut params = vec![
            ("output", "json".to_string()),
            ("n", STREAM_PAGE_SIZE.to_string()),
        ];
        if let Some(xt) = xt {
            params.push(("xt", xt.to_string()));
        }
        if let Some(c) = &cont {
            params.push(("c", c.clone()));
        }
        let page: Contents = sync_json(session.get(http, &path).query(&params)).await?;
        let empty = page.items.is_empty();
        for item in page.items {
            if !seen_ids.insert(item.id.clone()) {
                return Err(incomplete_stream());
            }
            out.push(item.try_into()?);
        }
        match page.continuation {
            Some(c) if !c.is_empty() => {
                if empty || !seen_tokens.insert(c.clone()) || page_index + 1 == MAX_STREAM_PAGES {
                    return Err(incomplete_stream());
                }
                cont = Some(c);
            }
            _ => break,
        }
    }
    Ok(out)
}

/// Flush addressable edits. Unmapped edits stay queued; failed pushes are
/// requeued before any subsequent pull can reconcile their local state.
async fn push_sync_queue(db: &Db, session: &Session, http: &Client) -> AppResult<()> {
    // 1 ── push: flush queued local read/starred changes. `take_sync_queue`
    // removes the rows up front, so any push that fails must be re-queued —
    // otherwise a network blip silently drops the user's change forever.
    let queue = {
        let conn = db.lock().await;
        db::take_sync_queue(&conn)?
    };
    let mut failed: Vec<db::SyncEntry> = Vec::new();
    for entry in queue {
        let tag = if entry.field == "starred" {
            STARRED_TAG
        } else {
            READ_TAG
        };
        let action = if entry.value { "a" } else { "r" };
        let pushed = session
            .post(http, "edit-tag")
            .form(&[
                ("i", entry.remote_id.as_str()),
                (action, tag),
                ("T", session.token.as_str()),
            ])
            .send()
            .await
            .and_then(|r| r.error_for_status());
        let pushed = match pushed {
            Ok(response) => response
                .text()
                .await
                .map(|body| body.trim() == "OK")
                .unwrap_or(false),
            Err(_) => false,
        };
        if !pushed {
            failed.push(entry);
        }
    }
    if !failed.is_empty() {
        log::warn!("sync: {} change(s) failed to push, re-queued", failed.len());
        let conn = db.lock().await;
        for entry in &failed {
            db::requeue_sync(&conn, entry.article_id, &entry.field, entry.value)?;
        }
    }

    Ok(())
}

/// Push queued changes, then pull subscriptions and read/starred state.
/// Returns the number of local articles whose state was reconciled.
pub async fn sync_now(db: &Db, http: &Client) -> AppResult<usize> {
    let creds = creds(db)
        .await?
        .ok_or_else(|| AppError::code("freshrssNotConnected"))?;
    let base = greader_base(&creds.url, creds.provider);
    let session = match &creds.auth {
        Some(auth) => session_with_token(http, &base, auth.clone()).await?,
        None => {
            // Legacy install: exchange the plaintext password for a token,
            // then migrate so the password is no longer kept on disk.
            let pass = creds.legacy_pass.as_deref().unwrap_or_default();
            let session = login(http, &base, &creds.user, pass).await?;
            persist_session(db, &creds.url, &creds.user, &session.auth, creds.provider).await?;
            session
        }
    };

    // Push known IDs first: an expensive/failed lookup must not hold them up.
    push_sync_queue(db, &session, http).await?;
    if matches!(creds.provider, Provider::Miniflux) {
        match resolve_pending_miniflux_ids(db, &session, http).await {
            Ok(mapped) if mapped > 0 => push_sync_queue(db, &session, http).await?,
            Ok(_) => {}
            Err(AppError::Db(error)) => return Err(AppError::Db(error)),
            Err(error) => {
                // Optional ID discovery is not an authoritative state set. A
                // failure leaves the unresolved queue intact and must not stop
                // normal unread/starred synchronization for other articles.
                log::warn!(
                    "sync: remote ID lookup failed; unmapped local edits remain queued: {error}"
                );
            }
        }
    }

    // 2 ── pull subscriptions: subscribe locally to any feed we don't have.
    let subs: SubList = sync_json(session.get(http, "subscription/list?output=json")).await?;
    let server_urls: std::collections::HashSet<String> = subs
        .subscriptions
        .iter()
        .filter_map(|s| s.url.clone())
        .filter(|u| !u.is_empty())
        .collect();
    {
        let conn = db.lock().await;
        for sub in subs.subscriptions {
            // Resolve the server-side folder (GReader "label") before moving
            // `url` out of `sub`, mapping it onto a local folder by name.
            let folder_id = sub
                .categories
                .iter()
                .find_map(SubCat::folder_name)
                .map(|name| db::folder_id_by_name(&conn, &name))
                .transpose()?;
            let Some(feed_url) = sub.url.filter(|u| !u.is_empty()) else {
                continue;
            };
            match db::find_feed_by_url(&conn, &feed_url)? {
                None => {
                    let title = sub.title.unwrap_or_else(|| feed_url.clone());
                    let st = parse::detect_source_type(&feed_url);
                    let _ = db::insert_feed(&conn, &feed_url, None, &title, None, st, folder_id);
                }
                // Reconcile the folder for a feed we already track, but only
                // when it isn't filed locally yet — don't yank a feed the user
                // has organised by hand back into the server's folder.
                Some(id) => {
                    if let Some(folder_id) = folder_id {
                        if db::feed_folder_id(&conn, id)?.is_none() {
                            db::move_feed(&conn, id, Some(folder_id))?;
                        }
                    }
                }
            }
        }
    }

    // 2b ── push subscriptions: subscribe the server to any local feed it
    // doesn't have yet, so adding a feed in the app propagates to the server
    // instead of leaving the two sides to drift. Best-effort and idempotent —
    // re-subscribing a feed the server already has is a no-op there.
    let local_feeds = {
        let conn = db.lock().await;
        db::feed_urls_for_sync(&conn)?
    };
    for url in feeds_to_push(&local_feeds, &server_urls) {
        let stream = format!("feed/{url}");
        let pushed = session
            .post(http, "subscription/edit")
            .form(&[
                ("ac", "subscribe"),
                ("s", stream.as_str()),
                ("T", session.token.as_str()),
            ])
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .is_ok();
        if !pushed {
            log::warn!("sync: failed to subscribe server to {url}");
        }
    }

    // 3 ── reconcile read/starred state, treating the server as the source of
    // truth. Papr polls each subscribed feed's raw RSS independently and marks
    // every fetched item unread, so the local unread counts drift far above the
    // server's. Rather than pull the (unbounded) full history, fetch just the
    // server's *unread* and *starred* sets — both small — then mark every other
    // synced article read. This is what a pure GReader client effectively does,
    // and it fixes the runaway unread counts of issue #96 where the old
    // "reconcile the most recent 1000 items" pass never reached the long tail.
    let unread =
        fetch_stream_items(&session, http, creds.provider, READING_LIST, Some(READ_TAG)).await?;
    let starred = fetch_stream_items(&session, http, creds.provider, STARRED_TAG, None).await?;

    let unread_urls: std::collections::HashSet<String> =
        unread.iter().map(|i| i.url.clone()).collect();
    let starred_urls: std::collections::HashSet<String> =
        starred.iter().map(|i| i.url.clone()).collect();

    let reconciled = {
        let conn = db.lock().await;
        // Assign remote ids for the items we did fetch, so a later local edit on
        // one of them has an id to push. (Read articles in the long tail carry
        // no id until they next appear in an unread/starred response — pushing a
        // change on those waits, exactly as before this change.)
        for item in unread.iter().chain(starred.iter()) {
            if let Some(aid) = db::article_id_by_url(&conn, &item.url)? {
                db::set_remote_id(&conn, aid, &item.id)?;
            }
        }
        // Scope the read/starred sweep to feeds the server actually knows about,
        // so a local-only feed not yet mirrored server-side isn't wrongly marked
        // all-read. Articles with an un-pushed local edit are left untouched.
        let server_feed_ids = db::feed_ids_by_urls(&conn, &server_urls)?;
        db::reconcile_sync_state(&conn, &server_feed_ids, &unread_urls, &starred_urls)?
    };
    Ok(reconciled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat(id: &str, label: Option<&str>) -> SubCat {
        SubCat {
            id: id.to_string(),
            label: label.map(str::to_string),
        }
    }

    #[test]
    fn folder_name_prefers_label() {
        assert_eq!(
            cat("user/-/label/Tech", Some("Tech"))
                .folder_name()
                .as_deref(),
            Some("Tech")
        );
    }

    #[test]
    fn folder_name_falls_back_to_label_id() {
        // Some servers omit the human label; derive it from the id instead.
        assert_eq!(
            cat("user/-/label/科技", None).folder_name().as_deref(),
            Some("科技")
        );
    }

    #[test]
    fn folder_name_skips_unnamed_categories() {
        // A state tag (not a label) or a blank label is not a folder.
        assert_eq!(
            cat("user/-/state/com.google/read", None).folder_name(),
            None
        );
        assert_eq!(cat("", Some("   ")).folder_name(), None);
    }

    #[test]
    fn folder_name_skips_freshrss_uncategorized() {
        // FreshRSS's built-in "Uncategorized" label is not a real folder, by
        // either label or id, and regardless of case.
        assert_eq!(
            cat("user/-/label/Uncategorized", Some("Uncategorized")).folder_name(),
            None
        );
        assert_eq!(cat("user/-/label/uncategorized", None).folder_name(), None);
    }

    #[test]
    fn feeds_to_push_selects_only_local_only_feeds() {
        let local = vec![
            "https://a.example/feed".to_string(),
            "https://b.example/feed".to_string(),
            "https://c.example/feed".to_string(),
        ];
        let server: std::collections::HashSet<String> =
            ["https://b.example/feed".to_string()].into_iter().collect();
        assert_eq!(
            feeds_to_push(&local, &server),
            vec!["https://a.example/feed", "https://c.example/feed"]
        );
    }

    #[test]
    fn feeds_to_push_empty_when_server_has_everything() {
        let local = vec!["https://a.example/feed".to_string()];
        let server: std::collections::HashSet<String> =
            ["https://a.example/feed".to_string()].into_iter().collect();
        assert!(feeds_to_push(&local, &server).is_empty());
    }
}

#[cfg(test)]
#[path = "sync_tests.rs"]
mod protocol_tests;
