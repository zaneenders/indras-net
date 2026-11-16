import ArgumentParser

struct Agent: ParsableCommand {
  static let configuration = CommandConfiguration(
    abstract: "Print concise setup and transfer instructions for agents.")

  func run() { print(AgentGuide.text) }
}

enum AgentGuide {
  static let text = #"""
    # Setu

    Transfer files between reachable peers through a persistent local node. Run as
    the node's OS user. Start with `setu status` and `setu jobs`; add `--socket PATH`
    for a custom socket. Use `setu COMMAND --help` for options and defaults.

    Setu uses framed TCP without authentication or encryption. Anyone who can reach
    the peer port can list, download, upload shared files, and restart the node. Use Tailscale or
    another trusted network or encrypted tunnel to control access.

    ## Transfers

    PEER is the remote IP address or DNS name.
    Short forms are `-H` for `--host`, `-D` for `--destination`, and `-C` for `--create`.

    ```sh
    setu list -H PEER
    setu send /absolute/file -H PEER
    setu send /absolute/file -H PEER -D /absolute/remote/directory
    setu send /absolute/file -H PEER -D /absolute/remote/new/directory -C
    setu get FILE_ID -H PEER --output /absolute/destination
    setu peers --tailscale --json
    setu send /absolute/file -H tower -H pi --json-stream
    ```

    Use a fresh listed FILE_ID and the peer's `--port` (default 8443). Keep sources
    unchanged; destinations cannot be overwritten. File IDs identify names, not content
    hashes. Omitting `--output` uses the shared directory. Transfers wait for verification;
    use their native waiting and SHA-256 receipts instead of writing polling or checksum
    wrapper scripts. `--no-wait` returns a job ID. Use
    `setu wait JOB_ID` to confirm completion or `setu cancel JOB_ID` to cancel.
    Use `setu job JOB_ID` for a snapshot, including progress and failure details.
    Add `--json` to status, list, jobs, job, send, get, wait, or cancel for a single
    JSON document with `schemaVersion: 1`. `--json-stream` instead emits newline-delimited
    progress and a final result. Do not combine the two flags. Waiting transfers return the final job;
    failed/cancelled transfers exit nonzero. With `--no-wait`, poll `job` or use `wait`.
    Human progress goes to stderr; JSON mode suppresses it. Byte counts and speed
    in `progress` describe its current phase; upload verification is indeterminate.
    Successful results include `receipt` with the verified SHA-256, size, peer, and
    destination (the path may be unavailable from an older receiver).

    Repeat `send -H` (or `send --host`) for multiple recipients. `--parallel` bounds active recipients
    (default 3, maximum 8). One failure does not stop the others; the final exit status
    is nonzero if any recipient failed. Retry failed recipients by sending to those hosts
    again; completed uploads are checked and reused. For `--no-wait`, the number of
    recipients must fit within `--parallel`.
    Status reads retry up to three times. If local control remains unavailable, no more
    recipients are queued; use the reported job IDs with `wait` after reconnecting.
    Submissions are never replayed after a lost reply because the job may already be running.

    Uploads use the receiver's shared directory, configured with `node --directory`.
    `send -D /absolute/remote/directory` chooses an existing directory on
    the receiver and keeps the source filename. The path is interpreted on the receiver;
    destination paths require updated sender and receiver nodes.
    Add `-C` to create the destination directory and missing parents before uploading.
    It requires `-D` and updated sender and receiver nodes; without it the directory must exist.
    For batch sends, the same `-D` path applies on every receiver; repeat `-H` for each recipient:

    ```sh
    setu send /absolute/file -H tower -H pi -D /absolute/remote/directory -C --json-stream
    ```

    `peers --tailscale` reads the signed-in local client's device list and probes Setu;
    it does not transfer files. Use `peers -H PEER` without Tailscale, or provide
    `--tailscale-executable /absolute/path` if automatic CLI lookup fails.

    A failed download with `job.recovery` retains verified bytes for 15 minutes, within
    a shared 1 GiB budget across all peers. Configure `node` or its service printer with
    `--download-recovery-mib N`; `0` disables retention. Oldest recoveries are evicted to
    make room; files larger than the budget are discarded with an explanatory job error.
    Use `setu finish JOB_ID --output /absolute/path`
    to retry placement without network transfer, or `setu cancel JOB_ID` to discard it.
    Omitting `finish --output` retries the original destination, including the shared-directory default.
    Expiry, eviction, and node restarts remove retained data. Publication checks run before
    receiving payloads, but permissions or competing writers can still change afterward.
    Node restarts discard jobs and progress; retry interrupted transfers from scratch.
    Transient peer requests retry up to three attempts with backoff. Incoming uploads
    expire after 900 idle seconds (checked every 30 seconds), with a limit of 128 active
    uploads. Override with `--upload-idle-timeout-seconds` and `--max-active-uploads`
    on `node` or the service printers. Receiving/publishing uploads do not expire.

    For remote recovery, run `setu restart -H PEER` (optional `--port` and `--json`).
    This sends a framed restart request directly; no local node or SSH is required. The peer
    acknowledges, then exits with status 1. systemd/launchd must be configured to restart
    it; a foreground node simply exits. Active transfers are interrupted; startup clears
    abandoned payloads and preserves completed files. The request is sent once, without
    retries; check peer health or list after the supervisor's restart delay.

    ## Setup

    Inspect existing services; preserve configuration. Check jobs before
    stopping a node to replace its binary. Build with Swift 6.4 on Linux/macOS 27:
    `swift build -c release --product setu --force-resolved-versions`; install on PATH.

    Set `setu_host` to this node's listen address and `setu_temporary` to a private
    absolute directory (0700), separate from shared files. Startup clears
    abandoned payloads there. Service printers use the running executable; supply
    `--executable /installed/setu` when generating from a build. Override `--directory`,
    `--socket`, and `--port` as needed, using absolute paths.

    Linux:
    ```sh
    mkdir -p "$HOME/.config/systemd/user"
    setu setup print-systemd -H "$setu_host" --temporary-directory "$setu_temporary" \
      > "$HOME/.config/systemd/user/setu.service"
    systemctl --user daemon-reload
    systemctl --user enable --now setu.service
    journalctl --user -u setu.service -n 50 --no-pager
    ```
    For active services, use `systemctl --user restart setu.service` after updates.
    Boot/logout operation requires `sudo loginctl enable-linger "$(id -un)"`.

    macOS:
    ```sh
    setu_plist="$HOME/Library/LaunchAgents/com.zaneenders.setu.plist"
    mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs/setu"
    chmod 700 "$HOME/Library/Logs/setu"
    setu setup print-launchd -H "$setu_host" --temporary-directory "$setu_temporary" \
      > "$setu_plist"
    plutil -lint -- "$setu_plist"
    launchctl bootstrap "gui/$(id -u)" "$setu_plist"
    ```
    Before replacing a loaded agent, run
    `launchctl bootout "gui/$(id -u)/com.zaneenders.setu"`. LaunchAgents require login;
    logs are in `~/Library/Logs/setu/`.

    Verify `setu status`, then a small peer transfer. `setu setup skill` installs only
    the instruction stub; optionally select `--agent codex` or `--agent claude`.
    """#
}
