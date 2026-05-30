//! Gateway handlers for the self-update API.
//!
//! `GET  /api/update/check` — check for a newer release.
//! `POST /api/update/run`   — start the 6-phase update pipeline.

use super::AppState;
use super::api::require_auth;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use serde_json::json;
use std::sync::atomic::Ordering;

pub async fn handle_api_update_check(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_auth(&state, &headers) {
        return e;
    }

    match zeroclaw_runtime::updater::check(None).await {
        Ok(info) => (
            StatusCode::OK,
            Json(json!({
                "current_version": info.current_version,
                "latest_version": info.latest_version,
                "is_newer": info.is_newer,
                "download_url": info.download_url,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("update check failed: {e}") })),
        ),
    }
}

pub async fn handle_api_update_run(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_auth(&state, &headers) {
        return e;
    }

    // Single-flight guard — only one update at a time.
    if state
        .update_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "status": "already_in_progress" })),
        );
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        let result = zeroclaw_runtime::updater::run(None, Some(&state_clone.event_tx)).await;

        match result {
            Ok(new_version) => {
                let _ = state_clone.event_tx.send(json!({
                    "type": "update_complete",
                    "version": new_version,
                }));

                // Restart the daemon with the new binary.
                let exe = match std::env::current_exe() {
                    Ok(e) => e,
                    Err(_e) => {
                        zeroclaw_log::record!(
                            ERROR,
                            zeroclaw_log::Event::new("api_update", zeroclaw_log::Action::Note)
                                .with_outcome(zeroclaw_log::EventOutcome::Unknown),
                            "Failed to determine exe path for restart: {e}"
                        );
                        state_clone
                            .update_in_progress
                            .store(false, Ordering::SeqCst);
                        return;
                    }
                };

                let args: Vec<String> = std::env::args().collect();
                let _err = match std::process::Command::new(&exe).args(&args[1..]).spawn() {
                    Ok(_) => {
                        zeroclaw_log::record!(
                            INFO,
                            zeroclaw_log::Event::new("api_update", zeroclaw_log::Action::Note),
                            "Restarting daemon after update to v{new_version}"
                        );
                        // Give the new process a moment to start accepting connections.
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        std::process::exit(0);
                    }
                    Err(e) => e,
                };

                zeroclaw_log::record!(
                    ERROR,
                    zeroclaw_log::Event::new("api_update", zeroclaw_log::Action::Note)
                        .with_outcome(zeroclaw_log::EventOutcome::Unknown),
                    "Failed to restart daemon: {err}"
                );
                state_clone
                    .update_in_progress
                    .store(false, Ordering::SeqCst);
            }
            Err(e) => {
                let _ = state_clone.event_tx.send(json!({
                    "type": "update_failed",
                    "error": format!("{e}"),
                }));
                state_clone
                    .update_in_progress
                    .store(false, Ordering::SeqCst);
            }
        }
    });

    (StatusCode::ACCEPTED, Json(json!({ "status": "started" })))
}
