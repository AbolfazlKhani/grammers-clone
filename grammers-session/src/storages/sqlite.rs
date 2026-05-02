// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::sync::Mutex;

use futures_core::future::BoxFuture;
use sqlx::sqlite::SqliteRow;
use sqlx::{Connection, Row, SqliteConnection};
use tokio::sync::Mutex as AsyncMutex;

use crate::types::{
    ChannelKind, ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerKind, UpdateState,
    UpdatesState,
};
use crate::{DEFAULT_DC, KNOWN_DC_OPTIONS, Session};

const VERSION: i64 = 1;

struct Cache {
    pub home_dc: i32,
    pub dc_options: HashMap<i32, DcOption>,
}

/// SQLite-based storage. This is the recommended option.
pub struct SqliteSession {
    database: AsyncMutex<SqliteConnection>,
    cache: Mutex<Cache>,
}

#[repr(u8)]
enum PeerSubtype {
    UserSelf = 1,
    UserBot = 2,
    UserSelfBot = 3,
    Megagroup = 4,
    Broadcast = 8,
    Gigagroup = 12,
}

async fn init(db: &mut SqliteConnection) -> sqlx::Result<()> {
    let mut user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_optional(&mut *db)
        .await?
        .unwrap_or(0);
    if user_version == VERSION {
        return Ok(());
    }

    if user_version == 0 {
        migrate_v0_to_v1(&mut *db).await?;
        user_version += 1;
    }
    if user_version == VERSION {
        // Can't bind PRAGMA parameters, but `VERSION` is not user-controlled input.
        sqlx::query(&format!("PRAGMA user_version = {VERSION}"))
            .execute(&mut *db)
            .await?;
    }
    Ok(())
}

async fn migrate_v0_to_v1(db: &mut SqliteConnection) -> sqlx::Result<()> {
    let mut tx = db.begin().await?;
    sqlx::query(
        "CREATE TABLE dc_home (
            dc_id INTEGER NOT NULL,
            PRIMARY KEY(dc_id))",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE dc_option (
            dc_id INTEGER NOT NULL,
            ipv4 TEXT NOT NULL,
            ipv6 TEXT NOT NULL,
            auth_key BLOB,
            PRIMARY KEY (dc_id))",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE peer_info (
            peer_id INTEGER NOT NULL,
            hash INTEGER,
            subtype INTEGER,
            PRIMARY KEY (peer_id))",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE update_state (
            pts INTEGER NOT NULL,
            qts INTEGER NOT NULL,
            date INTEGER NOT NULL,
            seq INTEGER NOT NULL)",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE channel_state (
            peer_id INTEGER NOT NULL,
            pts INTEGER NOT NULL,
            PRIMARY KEY (peer_id))",
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

impl SqliteSession {
    /// Open a connection to the SQLite database at `path`,
    /// creating one if it doesn't exist.
    pub async fn open(url: &str) -> sqlx::Result<Self> {
        let mut db = SqliteConnection::connect(url).await?;
        init(&mut db).await?;

        let home_dc = sqlx::query("SELECT * FROM dc_home LIMIT 1")
            .fetch_optional(&mut db)
            .await?
            .map(|row| row.get(0))
            .unwrap_or(DEFAULT_DC);

        let dc_options = sqlx::query("SELECT * FROM dc_option")
            .fetch_all(&mut db)
            .await?
            .into_iter()
            .map(|row| {
                let dc_option = DcOption {
                    id: row.get(0),
                    ipv4: row.get::<'_, String, _>(1).parse().unwrap(),
                    ipv6: row.get::<'_, String, _>(2).parse().unwrap(),
                    auth_key: row
                        .get::<'_, Option<Vec<u8>>, _>(3)
                        .map(|auth_key| auth_key.try_into().unwrap()),
                };
                (dc_option.id, dc_option)
            })
            .collect();

        Ok(SqliteSession {
            database: AsyncMutex::new(db),
            cache: Mutex::new(Cache {
                home_dc,
                dc_options,
            }),
        })
    }
}

impl Session for SqliteSession {
    fn home_dc_id(&self) -> i32 {
        self.cache.lock().unwrap().home_dc
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, ()> {
        self.cache.lock().unwrap().home_dc = dc_id;
        Box::pin(async move {
            let mut db = self.database.lock().await;
            let mut tx = db.begin().await.unwrap();
            sqlx::query("DELETE FROM dc_home")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("INSERT INTO dc_home(dc_id) VALUES (?)")
                .bind(dc_id)
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        })
    }

    fn dc_option(&self, dc_id: i32) -> Option<DcOption> {
        self.cache
            .lock()
            .unwrap()
            .dc_options
            .get(&dc_id)
            .cloned()
            .or_else(|| {
                KNOWN_DC_OPTIONS
                    .iter()
                    .find(|dc_option| dc_option.id == dc_id)
                    .cloned()
            })
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, ()> {
        self.cache
            .lock()
            .unwrap()
            .dc_options
            .insert(dc_option.id, dc_option.clone());

        let dc_option = dc_option.clone();
        Box::pin(async move {
            let mut db = self.database.lock().await;
            sqlx::query(
                "INSERT OR REPLACE INTO dc_option
                (dc_id, ipv4, ipv6, auth_key)
                VALUES (?, ?, ?, ?)",
            )
            .bind(dc_option.id)
            .bind(dc_option.ipv4.to_string())
            .bind(dc_option.ipv6.to_string())
            .bind(dc_option.auth_key.map(|k| k.to_vec()))
            .execute(&mut *db)
            .await
            .unwrap();
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Option<PeerInfo>> {
        Box::pin(async move {
            let mut db = self.database.lock().await;
            let map_row = |row: SqliteRow| {
                let subtype = row.get::<'_, Option<i64>, _>(2).map(|s| s as u8);
                match peer.kind() {
                    PeerKind::User => PeerInfo::User {
                        id: PeerId::user_unchecked(row.get::<'_, i64, _>(0)).bare_id_unchecked(),
                        auth: row.get::<'_, Option<i64>, _>(1).map(PeerAuth::from_hash),
                        bot: subtype.map(|s| s & PeerSubtype::UserBot as u8 != 0),
                        is_self: subtype.map(|s| s & PeerSubtype::UserSelf as u8 != 0),
                    },
                    PeerKind::Chat => PeerInfo::Chat {
                        id: peer.bare_id_unchecked(),
                    },
                    PeerKind::Channel => PeerInfo::Channel {
                        id: peer.bare_id_unchecked(),
                        auth: row.get::<'_, Option<i64>, _>(1).map(PeerAuth::from_hash),
                        kind: subtype.and_then(|s| {
                            if (s & PeerSubtype::Gigagroup as u8) == PeerSubtype::Gigagroup as u8 {
                                Some(ChannelKind::Gigagroup)
                            } else if s & PeerSubtype::Broadcast as u8 != 0 {
                                Some(ChannelKind::Broadcast)
                            } else if s & PeerSubtype::Megagroup as u8 != 0 {
                                Some(ChannelKind::Megagroup)
                            } else {
                                None
                            }
                        }),
                    },
                }
            };

            if let Some(peer_id) = peer.bot_api_dialog_id() {
                sqlx::query("SELECT * FROM peer_info WHERE peer_id = ? LIMIT 1")
                    .bind(peer_id)
                    .fetch_optional(&mut *db)
                    .await
                    .unwrap()
                    .map(map_row)
            } else {
                sqlx::query("SELECT * FROM peer_info WHERE subtype & ? LIMIT 1")
                    .bind(PeerSubtype::UserSelf as i64)
                    .fetch_optional(&mut *db)
                    .await
                    .unwrap()
                    .map(map_row)
            }
        })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, ()> {
        let peer = peer.clone();
        Box::pin(async move {
            let peer = if let Some(mut existing_peer) = self.peer(peer.id()).await {
                existing_peer.extend_info(&peer);
                existing_peer
            } else {
                peer
            };

            let mut db = self.database.lock().await;
            let subtype = match peer {
                PeerInfo::User { bot, is_self, .. } => {
                    match (bot.unwrap_or_default(), is_self.unwrap_or_default()) {
                        (true, true) => Some(PeerSubtype::UserSelfBot),
                        (true, false) => Some(PeerSubtype::UserBot),
                        (false, true) => Some(PeerSubtype::UserSelf),
                        (false, false) => None,
                    }
                }
                PeerInfo::Chat { .. } => None,
                PeerInfo::Channel { kind, .. } => kind.map(|kind| match kind {
                    ChannelKind::Megagroup => PeerSubtype::Megagroup,
                    ChannelKind::Broadcast => PeerSubtype::Broadcast,
                    ChannelKind::Gigagroup => PeerSubtype::Gigagroup,
                }),
            };
            let peer_id = peer.id().bot_api_dialog_id_unchecked();
            let hash = peer.auth().map(|auth| auth.hash());
            let subtype = subtype.map(|s| s as i64);
            sqlx::query(
                "INSERT OR REPLACE INTO peer_info
                    (peer_id, hash, subtype)
                    VALUES (?, ?, ?)",
            )
            .bind(peer_id)
            .bind(hash)
            .bind(subtype)
            .execute(&mut *db)
            .await
            .unwrap();
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, UpdatesState> {
        Box::pin(async move {
            let mut db = self.database.lock().await;
            let mut state = sqlx::query("SELECT pts, qts, date, seq FROM update_state LIMIT 1")
                .fetch_optional(&mut *db)
                .await
                .unwrap()
                .map(|row| UpdatesState {
                    pts: row.get(0),
                    qts: row.get(1),
                    date: row.get(2),
                    seq: row.get(3),
                    channels: Vec::new(),
                })
                .unwrap_or_default();
            state.channels = sqlx::query("SELECT peer_id, pts FROM channel_state")
                .fetch_all(&mut *db)
                .await
                .unwrap()
                .into_iter()
                .map(|row| ChannelState {
                    id: row.get(0),
                    pts: row.get(1),
                })
                .collect();
            state
        })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut db = self.database.lock().await;
            let mut tx = db.begin().await.unwrap();

            match update {
                UpdateState::All(updates_state) => {
                    sqlx::query("DELETE FROM update_state")
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    sqlx::query(
                        "INSERT INTO update_state(pts, qts, date, seq)
                            VALUES (?, ?, ?, ?)",
                    )
                    .bind(updates_state.pts)
                    .bind(updates_state.qts)
                    .bind(updates_state.date)
                    .bind(updates_state.seq)
                    .execute(&mut *tx)
                    .await
                    .unwrap();

                    sqlx::query("DELETE FROM channel_state")
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    for channel in updates_state.channels {
                        sqlx::query("INSERT INTO channel_state(peer_id, pts) VALUES (?, ?)")
                            .bind(channel.id)
                            .bind(channel.pts)
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                    }
                }
                UpdateState::Primary { pts, date, seq } => {
                    let previous = sqlx::query("SELECT 1 FROM update_state LIMIT 1")
                        .fetch_optional(&mut *tx)
                        .await
                        .unwrap();

                    if previous.is_some() {
                        sqlx::query("UPDATE update_state SET pts = ?, date = ?, seq = ?")
                            .bind(pts)
                            .bind(date)
                            .bind(seq)
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                    } else {
                        sqlx::query(
                            "INSERT INTO update_state(pts, qts, date, seq)
                                VALUES (?, 0, ?, ?)",
                        )
                        .bind(pts)
                        .bind(date)
                        .bind(seq)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    }
                }
                UpdateState::Secondary { qts } => {
                    let previous = sqlx::query("SELECT 1 FROM update_state LIMIT 1")
                        .fetch_optional(&mut *tx)
                        .await
                        .unwrap();

                    if previous.is_some() {
                        sqlx::query("UPDATE update_state SET qts = ?")
                            .bind(qts)
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                    } else {
                        sqlx::query(
                            "INSERT INTO update_state(pts, qts, date, seq)
                                VALUES (0, ?, 0, 0)",
                        )
                        .bind(qts)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    }
                }
                UpdateState::Channel { id, pts } => {
                    sqlx::query(
                        "INSERT OR REPLACE INTO channel_state(peer_id, pts)
                            VALUES (?, ?)",
                    )
                    .bind(id)
                    .bind(pts)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
            }

            tx.commit().await.unwrap();
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    use {DcOption, KNOWN_DC_OPTIONS, PeerInfo, Session, UpdateState};

    use super::*;

    #[test]
    fn exercise_sqlite_session() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(do_exercise_sqlite_session());
    }

    async fn do_exercise_sqlite_session() {
        let session = SqliteSession::open(":memory:").await.unwrap();

        assert_eq!(session.home_dc_id(), DEFAULT_DC);
        session.set_home_dc_id(DEFAULT_DC + 1).await;
        assert_eq!(session.home_dc_id(), DEFAULT_DC + 1);

        assert_eq!(
            session.dc_option(KNOWN_DC_OPTIONS[0].id),
            Some(KNOWN_DC_OPTIONS[0].clone())
        );
        let new_dc_option = DcOption {
            id: KNOWN_DC_OPTIONS
                .iter()
                .map(|dc_option| dc_option.id)
                .max()
                .unwrap()
                + 1,
            ipv4: SocketAddrV4::new(Ipv4Addr::from_bits(0), 1),
            ipv6: SocketAddrV6::new(Ipv6Addr::from_bits(0), 1, 0, 0),
            auth_key: Some([1; 256]),
        };
        assert_eq!(session.dc_option(new_dc_option.id), None);
        session.set_dc_option(&new_dc_option).await;
        assert_eq!(session.dc_option(new_dc_option.id), Some(new_dc_option));

        assert_eq!(session.peer(PeerId::self_user()).await, None);
        assert_eq!(session.peer(PeerId::user_unchecked(1)).await, None);
        let peer = PeerInfo::User {
            id: 1,
            auth: None,
            bot: Some(true),
            is_self: Some(true),
        };
        session.cache_peer(&peer).await;
        assert_eq!(session.peer(PeerId::self_user()).await, Some(peer.clone()));
        assert_eq!(session.peer(PeerId::user_unchecked(1)).await, Some(peer));

        assert_eq!(session.peer(PeerId::channel_unchecked(1)).await, None);
        let peer = PeerInfo::Channel {
            id: 1,
            auth: Some(PeerAuth::from_hash(-1)),
            kind: Some(ChannelKind::Broadcast),
        };
        session.cache_peer(&peer).await;
        assert_eq!(session.peer(PeerId::channel_unchecked(1)).await, Some(peer));

        assert_eq!(session.updates_state().await, UpdatesState::default());
        session
            .set_update_state(UpdateState::All(UpdatesState {
                pts: 1,
                qts: 2,
                date: 3,
                seq: 4,
                channels: vec![
                    ChannelState { id: 5, pts: 6 },
                    ChannelState { id: 7, pts: 8 },
                ],
            }))
            .await;
        session
            .set_update_state(UpdateState::Primary {
                pts: 2,
                date: 4,
                seq: 5,
            })
            .await;
        session
            .set_update_state(UpdateState::Secondary { qts: 3 })
            .await;
        session
            .set_update_state(UpdateState::Channel { id: 7, pts: 9 })
            .await;
        assert_eq!(
            session.updates_state().await,
            UpdatesState {
                pts: 2,
                qts: 3,
                date: 4,
                seq: 5,
                channels: vec![
                    ChannelState { id: 5, pts: 6 },
                    ChannelState { id: 7, pts: 9 },
                ],
            }
        );
    }
}
