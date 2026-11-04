//! Local HTTP fixtures only: never open the user's database or contact a reader.
use super::*;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Request {
    method: String,
    path: String,
    params: Vec<(String, String)>,
}
impl Request {
    fn param(&self, key: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}
struct Server {
    base: String,
    task: tokio::task::JoinHandle<()>,
    checked: bool,
}
impl Server {
    async fn finish(mut self) {
        self.task.abort();
        let result = (&mut self.task).await;
        self.checked = true;
        if let Err(error) = result {
            assert!(error.is_cancelled(), "fixture server panicked: {error}");
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
        assert!(
            self.checked || std::thread::panicking(),
            "fixture task must be joined with finish()"
        );
    }
}
async fn server(handler: impl Fn(Request) -> (u16, String) + Send + Sync + 'static) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let (head_end, len) = loop {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
                if let Some(end) = data.windows(4).position(|s| s == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&data[..end]);
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (end + 4, len);
                }
            };
            while data.len() < head_end + len {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
            }
            let headers = String::from_utf8_lossy(&data[..head_end]);
            assert!(headers
                .to_ascii_lowercase()
                .contains("authorization: googlelogin auth=fixture"));
            let mut first = headers.lines().next().unwrap().split_whitespace();
            let method = first.next().unwrap().to_string();
            let url = url::Url::parse(&format!("http://fixture{}", first.next().unwrap())).unwrap();
            let mut params: Vec<_> = url.query_pairs().into_owned().collect();
            params
                .extend(url::form_urlencoded::parse(&data[head_end..head_end + len]).into_owned());
            let (status, body) = handler(Request {
                method,
                path: url.path().to_string(),
                params,
            });
            let response = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    Server {
        base,
        task,
        checked: false,
    }
}
fn ok(body: impl Into<String>) -> (u16, String) {
    (200, body.into())
}
fn item(id: u64) -> serde_json::Value {
    serde_json::json!({"id": format!("tag:google.com,2005:reader/item/{id:016x}"),
        "canonical": [{"href": format!("https://example.com/{id}")}]})
}
fn http() -> Client {
    Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap()
}
fn session(base: &str) -> Session {
    Session {
        base: base.into(),
        auth: "fixture".into(),
        token: "write-fixture".into(),
    }
}
async fn fixture_db(base: &str, provider: Provider) -> Db {
    let conn = db::open(std::path::Path::new(":memory:")).unwrap();
    conn.execute("INSERT INTO feeds(id, feed_url, title) VALUES (1, 'https://example.com/feed', 'Fixture'), (2, 'https://local.example/feed', 'Local')", []).unwrap();
    conn.execute("INSERT INTO articles(id,feed_id,guid,url,title,is_read,is_starred) VALUES (1,1,'one','https://example.com/42','One',0,1), (2,2,'local','https://local.example/1','Local',0,1)", []).unwrap();
    let db = Mutex::new(conn);
    persist_session(&db, base, "fixture", "fixture", provider)
        .await
        .unwrap();
    db
}
async fn flags(db: &Db) -> Vec<(i64, bool, bool)> {
    let conn = db.lock().await;
    let mut stmt = conn
        .prepare("SELECT id,is_read,is_starred FROM articles ORDER BY id")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn common(req: &Request) -> Option<(u16, String)> {
    if req.path.ends_with("/token") {
        return Some(ok("write-fixture"));
    }
    if req.path.ends_with("/subscription/list") {
        return Some(ok(
            r#"{"subscriptions":[{"url":"https://example.com/feed","title":"Fixture"}]}"#,
        ));
    }
    if req.path.ends_with("/subscription/edit") {
        return Some(ok("OK"));
    }
    None
}

#[tokio::test]
async fn miniflux_pages_ids_and_batches_contents_with_normalized_ids() {
    let srv = server(|req| {
        if req.path.ends_with("/items/ids") {
            assert_eq!(req.method, "GET");
            assert_eq!(req.param("s"), Some(READING_LIST));
            assert_eq!(req.param("xt"), Some(READ_TAG));
            assert_eq!(req.param("n"), Some("1000"));
            return match req.param("c") {
                None => ok(serde_json::json!({"itemRefs": (1..=101).map(|i| serde_json::json!({"id":i.to_string()})).collect::<Vec<_>>(), "continuation":"101"}).to_string()),
                Some("101") => ok(r#"{"itemRefs":[{"id":"102"}]}"#),
                _ => panic!("unexpected offset"),
            };
        }
        assert!(req.path.ends_with("/items/contents"));
        assert_eq!(req.method, "POST");
        assert_eq!(req.param("T"), Some("write-fixture"));
        let ids: Vec<u64> = req.params.iter().filter(|(k,_)| k == "i").map(|(_,v)| miniflux_content_id(v).unwrap()).collect();
        assert!(ids.len() == 100 || ids.len() == 2);
        // The server need not preserve the request order.
        ok(serde_json::json!({"items": ids.into_iter().rev().map(item).collect::<Vec<_>>()}).to_string())
    }).await;
    let result = fetch_stream_items(
        &session(&srv.base),
        &http(),
        Provider::Miniflux,
        READING_LIST,
        Some(READ_TAG),
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 102);
    assert!(result
        .iter()
        .any(|i| i.url == "https://example.com/42" && i.id.ends_with("000000000000002a")));
    srv.finish().await;
}

#[tokio::test]
async fn invalid_or_incomplete_miniflux_sync_preserves_both_states() {
    for scenario in [
        "array",
        "positional_array",
        "truncated_json",
        "missing_second_page",
        "numeric_continuation",
        "empty_continuation",
        "invalid_id",
        "content_positional_array",
        "missing_content_field",
        "missing",
        "null",
        "http",
        "gap",
        "repeat",
        "empty_page",
        "limit",
        "missing_content",
        "wrong_content",
        "duplicate_content",
        "missing_url",
        "content_array",
        "star_failure",
        "subscription_missing",
    ] {
        let srv = server(move |req| {
            if scenario == "subscription_missing" && req.path.ends_with("/subscription/list") { return ok("{}"); }
            if let Some(reply) = common(&req) { return reply; }
            if req.path.ends_with("/items/ids") {
                assert_eq!(req.method, "GET");
                if req.param("s") == Some(STARRED_TAG) {
                    return if scenario == "star_failure" { (503, "unavailable".into()) } else { ok(r#"{"itemRefs":[]}"#) };
                }
                return match scenario {
                    "array" => ok("[]"),
                    "positional_array" => ok("[[]]"),
                    "truncated_json" => ok(r#"{"itemRefs":["#),
                    "numeric_continuation" => ok(r#"{"itemRefs":[{"id":"42"}],"continuation":1}"#),
                    "empty_continuation" => ok(r#"{"itemRefs":[{"id":"42"}],"continuation":""}"#),
                    "invalid_id" => ok(r#"{"itemRefs":[{"id":"0"}]}"#),
                    "missing_second_page" => if req.param("c").is_none() { ok(r#"{"itemRefs":[{"id":"42"}],"continuation":"1"}"#) } else { (503,"unavailable".into()) }, "missing" => ok("{}"), "null" => ok(r#"{"itemRefs":null}"#),
                    "http" => (500, "error".into()),
                    "gap" => ok(r#"{"itemRefs":[{"id":"42"}],"continuation":"2"}"#),
                    "repeat" => ok(r#"{"itemRefs":[{"id":"42"}],"continuation":"1"}"#),
                    "empty_page" => ok(r#"{"itemRefs":[],"continuation":"1"}"#),
                    "limit" => {
                        let offset = req.param("c").unwrap_or("0").parse::<usize>().unwrap();
                        ok(serde_json::json!({"itemRefs":[{"id":(offset+1).to_string()}],"continuation":(offset+1).to_string()}).to_string())
                    }
                    "duplicate_content" => ok(r#"{"itemRefs":[{"id":"42"},{"id":"43"}]}"#),
                    _ => ok(r#"{"itemRefs":[{"id":"42"}]}"#),
                };
            }
            assert!(req.path.ends_with("/items/contents"));
            match scenario {
                "missing_content" => ok(r#"{"items":[]}"#),
                "wrong_content" => ok(serde_json::json!({"items":[item(99)]}).to_string()),
                "duplicate_content" => ok(serde_json::json!({"items":[item(42),item(42)]}).to_string()),
                "missing_url" => ok(r#"{"items":[{"id":"tag:google.com,2005:reader/item/000000000000002a"}]}"#),
                "content_array" => ok("[]"),
                "content_positional_array" => ok("[[]]"),
                "missing_content_field" => ok("{}"),
                _ => ok(serde_json::json!({"items":[item(42)]}).to_string()),
            }
        }).await;
        let db = fixture_db(&srv.base, Provider::Miniflux).await;
        let before = flags(&db).await;
        assert!(sync_now(&db, &http()).await.is_err(), "accepted {scenario}");
        assert_eq!(flags(&db).await, before, "changed state for {scenario}");
        srv.finish().await;
    }
}

#[tokio::test]
async fn miniflux_bidirectional_read_and_star_edits_and_failed_push_retry() {
    // Server starts with the fixture article unread and starred.
    let remote = Arc::new(StdMutex::new((false, true, false)));
    let state = remote.clone();
    let srv = server(move |req| {
        if let Some(reply) = common(&req) {
            return reply;
        }
        let mut state = state.lock().unwrap();
        if req.path.ends_with("/edit-tag") {
            assert_eq!(req.method, "POST");
            assert_eq!(
                req.param("i"),
                Some("tag:google.com,2005:reader/item/000000000000002a")
            );
            if state.2 {
                return ok("[]");
            } // HTTP 200 must not acknowledge an edit.
            let add = req.param("a");
            let remove = req.param("r");
            match add.or(remove).unwrap() {
                READ_TAG => state.0 = add.is_some(),
                STARRED_TAG => state.1 = add.is_some(),
                _ => panic!("unexpected tag"),
            }
            return ok("OK");
        }
        if req.path.ends_with("/items/ids") {
            let include = if req.param("s") == Some(STARRED_TAG) {
                state.1
            } else {
                req.param("xt").is_none() || !state.0
            };
            return ok(if include {
                r#"{"itemRefs":[{"id":"42"}]}"#
            } else {
                r#"{"itemRefs":[]}"#
            });
        }
        assert!(req.path.ends_with("/items/contents"));
        ok(serde_json::json!({"items":[item(42)]}).to_string())
    })
    .await;
    let db = fixture_db(&srv.base, Provider::Miniflux).await;
    sync_now(&db, &http()).await.unwrap(); // map server item to a remote ID
    for (read, star) in [(true, false), (false, true)] {
        {
            let conn = db.lock().await;
            db::set_read(&conn, 1, read).unwrap();
            db::set_starred(&conn, 1, star).unwrap();
            db::enqueue_sync(&conn, 1, "read", read).unwrap();
            db::enqueue_sync(&conn, 1, "starred", star).unwrap();
        }
        sync_now(&db, &http()).await.unwrap();
        assert_eq!(*remote.lock().unwrap(), (read, star, false));
        assert_eq!(flags(&db).await, vec![(1, read, star), (2, false, true)]);
        assert!(db::pending_sync_article_ids(&*db.lock().await)
            .unwrap()
            .is_empty());
    }
    // Server-side changes flow down, including a valid authoritative empty set.
    *remote.lock().unwrap() = (true, false, false);
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(flags(&db).await[0], (1, true, false));
    // A failed push preserves local edits and the retry queue despite stale pull.
    {
        let conn = db.lock().await;
        db::set_read(&conn, 1, false).unwrap();
        db::set_starred(&conn, 1, true).unwrap();
        db::enqueue_sync(&conn, 1, "read", false).unwrap();
        db::enqueue_sync(&conn, 1, "starred", true).unwrap();
    }
    remote.lock().unwrap().2 = true;
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(flags(&db).await[0], (1, false, true));
    assert_eq!(
        db::pending_sync_article_ids(&*db.lock().await).unwrap(),
        vec![1]
    );
    remote.lock().unwrap().2 = false;
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(*remote.lock().unwrap(), (false, true, false));
    assert!(db::pending_sync_article_ids(&*db.lock().await)
        .unwrap()
        .is_empty());
    srv.finish().await;
}

#[tokio::test]
async fn freshrss_pages_work_and_incomplete_pages_fail_closed() {
    for scenario in ["valid", "repeat", "limit", "failure", "missing", "array"] {
        let srv = server(move |req| {
            if let Some(reply) = common(&req) {
                return reply;
            }
            assert!(req
                .path
                .contains("/api/greader.php/reader/api/0/stream/contents/"));
            if req.path.ends_with(STARRED_TAG) {
                return ok(r#"{"items":[]}"#);
            }
            if scenario == "missing" {
                return ok("{}");
            }
            if scenario == "array" {
                return ok("[]");
            }
            let page = req.param("c").unwrap_or("0").parse::<u64>().unwrap();
            if scenario == "failure" && page > 0 {
                return (500, "error".into());
            }
            let continuation = match scenario {
                "repeat" => Some("1".to_string()),
                "limit" => Some((page + 1).to_string()),
                _ if page == 0 => Some("1".to_string()),
                _ => None,
            };
            let mut body = serde_json::json!({"items":[item(42+page)]});
            if let Some(c) = continuation {
                body["continuation"] = c.into();
            }
            ok(body.to_string())
        })
        .await;
        let db = fixture_db(&srv.base, Provider::FreshRss).await;
        let before = flags(&db).await;
        let result = sync_now(&db, &http()).await;
        if scenario == "valid" {
            result.unwrap();
            assert_eq!(flags(&db).await, vec![(1, false, false), (2, false, true)]);
        } else {
            assert!(result.is_err(), "accepted {scenario}");
            assert_eq!(flags(&db).await, before);
        }
        srv.finish().await;
    }
}

/// Model a fresh RSS ingestion into Papr's own database, without fetching a feed.
async fn ingest_new_article(db: &Db, id: u64) -> i64 {
    let conn = db.lock().await;
    let url = format!("https://example.com/{id}");
    let article = crate::db::NewArticle {
        guid: format!("new-{id}"),
        url: Some(url.clone()),
        title: "New synthetic article".into(),
        author: None,
        summary: None,
        content_html: None,
        body_text: String::new(),
        image_url: None,
        published_at: None,
        enclosures: Vec::new(),
    };
    db::upsert_article(&conn, 1, &article, false, &[]).unwrap();
    db::article_id_by_url(&conn, &url).unwrap().unwrap()
}

#[tokio::test]
async fn newly_ingested_unread_article_maps_and_pushes_early_edits() {
    let remote = Arc::new(StdMutex::new((false, false)));
    let state = remote.clone();
    let srv = server(move |req| {
        if let Some(reply) = common(&req) {
            return reply;
        }
        let mut state = state.lock().unwrap();
        if req.path.ends_with("/edit-tag") {
            assert_eq!(
                req.param("i"),
                Some("tag:google.com,2005:reader/item/000000000000002b")
            );
            let add = req.param("a");
            match add.or(req.param("r")).unwrap() {
                READ_TAG => state.0 = add.is_some(),
                STARRED_TAG => state.1 = add.is_some(),
                _ => panic!("unexpected edit"),
            }
            return ok("OK");
        }
        if req.path.ends_with("/items/ids") {
            let include = if req.param("s") == Some(STARRED_TAG) {
                state.1
            } else {
                req.param("xt").is_none() || !state.0
            };
            return ok(if include {
                r#"{"itemRefs":[{"id":"43"}]}"#
            } else {
                r#"{"itemRefs":[]}"#
            });
        }
        assert!(req.path.ends_with("/items/contents"));
        ok(serde_json::json!({"items":[item(43)]}).to_string())
    })
    .await;
    let db = fixture_db(&srv.base, Provider::Miniflux).await;
    // The remote has the article before Papr ingests it; state sync does not import it.
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(flags(&db).await.len(), 2);
    let aid = ingest_new_article(&db, 43).await;
    // User edits it before its first sync, while it has no remote ID yet.
    {
        let conn = db.lock().await;
        db::set_read(&conn, aid, true).unwrap();
        db::set_starred(&conn, aid, true).unwrap();
        db::enqueue_sync(&conn, aid, "read", true).unwrap();
        db::enqueue_sync(&conn, aid, "starred", true).unwrap();
    }
    sync_now(&db, &http()).await.unwrap(); // resolve ID before pushing the early edit
    assert_eq!(*remote.lock().unwrap(), (true, true));
    assert!(db::pending_sync_article_ids(&*db.lock().await)
        .unwrap()
        .is_empty());
    *remote.lock().unwrap() = (false, false); // another client changes both flags
    sync_now(&db, &http()).await.unwrap();
    assert!(flags(&db).await.contains(&(aid, false, false)));
    srv.finish().await;
}

#[tokio::test]
async fn remote_only_starred_article_is_not_imported_by_state_reconciliation() {
    let srv = server(|req| {
        if let Some(reply) = common(&req) {
            return reply;
        }
        if req.path.ends_with("/items/ids") {
            return ok(if req.param("s") == Some(STARRED_TAG) {
                r#"{"itemRefs":[{"id":"43"}]}"#
            } else {
                r#"{"itemRefs":[]}"#
            });
        }
        assert!(req.path.ends_with("/items/contents"));
        ok(serde_json::json!({"items":[item(43)]}).to_string())
    })
    .await;
    let db = fixture_db(&srv.base, Provider::Miniflux).await;
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(
        db::article_id_by_url(&*db.lock().await, "https://example.com/43").unwrap(),
        None
    );
    // If RSS later provides the article, its star state is then reconciled.
    let aid = ingest_new_article(&db, 43).await;
    sync_now(&db, &http()).await.unwrap();
    assert!(flags(&db).await.contains(&(aid, true, true)));
    srv.finish().await;
}

#[tokio::test]
async fn new_read_unstarred_article_resolves_pending_id_and_pushes() {
    let remote = Arc::new(StdMutex::new((true, false)));
    let state = remote.clone();
    let srv = server(move |req| {
        if let Some(reply) = common(&req) {
            return reply;
        }
        let mut state = state.lock().unwrap();
        if req.path.ends_with("/edit-tag") {
            assert_eq!(
                req.param("i"),
                Some("tag:google.com,2005:reader/item/000000000000002b")
            );
            let add = req.param("a");
            match add.or(req.param("r")).unwrap() {
                READ_TAG => state.0 = add.is_some(),
                STARRED_TAG => state.1 = add.is_some(),
                _ => panic!("unexpected edit"),
            }
            return ok("OK");
        }
        if req.path.ends_with("/items/ids") {
            let include = if req.param("s") == Some(STARRED_TAG) {
                state.1
            } else {
                req.param("xt").is_none() || !state.0
            };
            return ok(if include {
                r#"{"itemRefs":[{"id":"43"}]}"#
            } else {
                r#"{"itemRefs":[]}"#
            });
        }
        assert!(req.path.ends_with("/items/contents"));
        ok(serde_json::json!({"items":[item(43)]}).to_string())
    })
    .await;
    let db = fixture_db(&srv.base, Provider::Miniflux).await;
    let aid = ingest_new_article(&db, 43).await;
    sync_now(&db, &http()).await.unwrap();
    assert!(flags(&db).await.contains(&(aid, true, false)));
    {
        let conn = db.lock().await;
        db::set_read(&conn, aid, false).unwrap();
        db::set_starred(&conn, aid, true).unwrap();
        db::enqueue_sync(&conn, aid, "read", false).unwrap();
        db::enqueue_sync(&conn, aid, "starred", true).unwrap();
    }
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(*remote.lock().unwrap(), (false, true));
    assert!(flags(&db).await.contains(&(aid, false, true)));
    assert!(db::pending_sync_article_ids(&*db.lock().await)
        .unwrap()
        .is_empty());
    srv.finish().await;
}

#[tokio::test]
#[should_panic(expected = "fixture server panicked")]
async fn fixture_task_panic_cannot_count_as_an_expected_sync_error() {
    let srv = server(|_| panic!("deliberate fixture assertion failure")).await;
    // A negative test might accept this transport error. Joining the task must
    // still fail the test instead of silently swallowing the server's panic.
    let _ = session(&srv.base).get(&http(), "fixture").send().await;
    srv.finish().await;
}

#[tokio::test]
async fn sixteen_digit_decimal_reference_is_posted_as_long_form_hex() {
    const ID: u64 = 1_234_567_890_123_456;
    let srv = server(|req| {
        if req.path.ends_with("/items/ids") {
            return ok(serde_json::json!({"itemRefs":[{"id": ID.to_string()}]}).to_string());
        }
        assert!(req.path.ends_with("/items/contents"));
        let sent = req.param("i").unwrap();
        assert_eq!(sent, format!("tag:google.com,2005:reader/item/{ID:016x}"));
        assert_eq!(miniflux_content_id(sent).unwrap(), ID);
        // A bare 16-digit decimal string would be interpreted by Miniflux as
        // hexadecimal and point at a different (or nonexistent) article.
        assert_ne!(u64::from_str_radix(&ID.to_string(), 16).unwrap(), ID);
        ok(serde_json::json!({"items":[item(ID)]}).to_string())
    })
    .await;
    let items = fetch_miniflux_items(&session(&srv.base), &http(), READING_LIST, None)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].url, format!("https://example.com/{ID}"));
    srv.finish().await;
}

#[tokio::test]
async fn changed_membership_during_offset_pagination_aborts_state_sweep() {
    let scans = Arc::new(StdMutex::new(0));
    let state = scans.clone();
    let srv = server(move |req| {
        if let Some(reply) = common(&req) {
            return reply;
        }
        if req.path.ends_with("/items/ids") {
            assert_eq!(req.param("s"), Some(READING_LIST));
            if req.param("c") == Some("1") {
                // Item 42 was read between requests. Item 43 shifted to offset
                // zero, so requesting offset one misses it without an error.
                return ok(r#"{"itemRefs":[]}"#);
            }
            let mut scans = state.lock().unwrap();
            *scans += 1;
            return if *scans == 1 {
                ok(r#"{"itemRefs":[{"id":"42"}],"continuation":"1"}"#)
            } else {
                ok(r#"{"itemRefs":[{"id":"43"}]}"#)
            };
        }
        assert!(req.path.ends_with("/items/contents"));
        ok(serde_json::json!({"items":[item(42)]}).to_string())
    })
    .await;
    let db = fixture_db(&srv.base, Provider::Miniflux).await;
    let before = flags(&db).await;
    assert!(sync_now(&db, &http()).await.is_err());
    assert_eq!(flags(&db).await, before);
    assert_eq!(*scans.lock().unwrap(), 2);
    srv.finish().await;
}

#[tokio::test]
async fn pending_article_missing_remotely_is_retained_without_guessing_an_id() {
    let srv = server(|req| {
        if let Some(reply) = common(&req) {
            return reply;
        }
        assert!(
            req.path.ends_with("/items/ids"),
            "unresolved ID must not be pushed"
        );
        ok(r#"{"itemRefs":[]}"#)
    })
    .await;
    let db = fixture_db(&srv.base, Provider::Miniflux).await;
    let aid = ingest_new_article(&db, 43).await;
    {
        let conn = db.lock().await;
        db::set_starred(&conn, aid, true).unwrap();
        db::enqueue_sync(&conn, aid, "starred", true).unwrap();
    }
    sync_now(&db, &http()).await.unwrap();
    assert_eq!(
        db::pending_sync_article_ids(&*db.lock().await).unwrap(),
        vec![aid]
    );
    assert!(flags(&db).await.contains(&(aid, false, true)));
    srv.finish().await;
}

#[tokio::test]
async fn failed_unmapped_lookup_does_not_block_known_pushes_or_other_state_pulls() {
    for lookup_failure in ["limit", "http", "malformed"] {
        for fail_known_push in [false, true] {
            let events = Arc::new(StdMutex::new(Vec::<String>::new()));
            let recorded = events.clone();
            let srv = server(move |req| {
                if let Some(reply) = common(&req) { return reply; }
                if req.path.ends_with("/edit-tag") {
                    assert_eq!(req.param("i"), Some("tag:google.com,2005:reader/item/000000000000002a"));
                    recorded.lock().unwrap().push("push-known".into());
                    return if fail_known_push { (503, "unavailable".into()) } else { ok("OK") };
                }
                if req.path.ends_with("/items/ids") {
                    if req.param("s") == Some(READING_LIST) && req.param("xt").is_none() {
                        recorded.lock().unwrap().push("lookup-unmapped".into());
                        return match lookup_failure {
                            "http" => (503, "unavailable".into()),
                            "malformed" => ok("[]"),
                            "limit" => {
                                let offset = req.param("c").unwrap_or("0").parse::<usize>().unwrap();
                                // A complete 30,000 references with more history
                                // remaining: discovery must not apply partial IDs.
                                ok(serde_json::json!({
                                    "itemRefs": (offset+100..offset+1100).map(|id| serde_json::json!({"id":id.to_string()})).collect::<Vec<_>>(),
                                    "continuation": (offset+1000).to_string()
                                }).to_string())
                            },
                            _ => unreachable!(),
                        };
                    }
                    recorded.lock().unwrap().push("pull-state".into());
                    let mut ids = vec![serde_json::json!({"id":"44"})];
                    if fail_known_push { ids.push(serde_json::json!({"id":"42"})); }
                    return ok(serde_json::json!({"itemRefs":ids}).to_string());
                }
                assert!(req.path.ends_with("/items/contents"));
                let ids = req.params.iter().filter(|(k,_)| k == "i")
                    .map(|(_,id)| item(miniflux_content_id(id).unwrap())).collect::<Vec<_>>();
                ok(serde_json::json!({"items":ids}).to_string())
            }).await;
            let db = fixture_db(&srv.base, Provider::Miniflux).await;
            let unresolved = ingest_new_article(&db, 43).await;
            let pulled = ingest_new_article(&db, 44).await;
            {
                let conn = db.lock().await;
                db::set_remote_id(&conn, 1, "tag:google.com,2005:reader/item/000000000000002a")
                    .unwrap();
                db::set_read(&conn, 1, true).unwrap();
                db::set_starred(&conn, 1, false).unwrap();
                db::enqueue_sync(&conn, 1, "read", true).unwrap();
                db::enqueue_sync(&conn, 1, "starred", false).unwrap();
                db::set_starred(&conn, unresolved, true).unwrap();
                db::enqueue_sync(&conn, unresolved, "starred", true).unwrap();
            }
            sync_now(&db, &http()).await.unwrap();
            let events = events.lock().unwrap();
            assert_eq!(&events[..2], &["push-known", "push-known"]);
            assert_eq!(events[2], "lookup-unmapped");
            assert_eq!(
                events.iter().filter(|event| *event == "push-known").count(),
                2
            );
            assert!(events.iter().any(|event| event == "pull-state"));
            drop(events);
            let expected: std::collections::HashSet<_> = if fail_known_push {
                [1, unresolved].into_iter().collect()
            } else {
                [unresolved].into_iter().collect()
            };
            assert_eq!(
                db::pending_sync_article_ids(&*db.lock().await)
                    .unwrap()
                    .into_iter()
                    .collect::<std::collections::HashSet<_>>(),
                expected
            );
            let states = flags(&db).await;
            assert!(
                states.contains(&(1, true, false)),
                "known edit must remain local even if its push failed"
            );
            assert!(
                states.contains(&(unresolved, false, true)),
                "unresolved edit must remain pending and intact"
            );
            assert!(
                states.contains(&(pulled, false, true)),
                "unrelated state pulls must still reconcile"
            );
            let id: Option<String> = db
                .lock()
                .await
                .query_row(
                    "SELECT remote_id FROM articles WHERE id=?1",
                    [unresolved],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(id, None);
            srv.finish().await;
        }
    }
}
