# MassChange: docker-compose to rootless podman/Quadlet

Target: RHEL 9.7, podman 5.6.0, rootless, SELinux non-enforcing.

## Before you touch anything: four preflight checks

These are the failures that waste a day if you skip them.

**1. The service user's home must not be on NFS.** Rootless podman's image store
requires overlayfs, which is unsupported over NFS.

```bash
stat -f -c %T ~                          # must NOT be nfs
stat -f -c %T ~/.local/share/containers  # ditto (create the dir first if absent)
```

If home is NFS-mounted, point the graphroot at local disk before doing anything
else, in `~/.config/containers/storage.conf`:

```toml
[storage]
driver = "overlay"
graphroot = "/var/lib/mango-containers/storage"   # local disk, owned by the service user
runroot  = "/run/user/1000/containers"
```

**2. Subordinate ID ranges must exist for the service user.**

```bash
grep "^$USER:" /etc/subuid /etc/subgid
```

Two lines, typically `user:100000:65536`. If absent:
`sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 "$USER"`
then `podman system migrate`.

**3. Lingering must be enabled, or nothing starts at boot** — and everything dies
when you log out, which is a confusing way to discover this.

```bash
sudo loginctl enable-linger "$USER"
loginctl show-user "$USER" --property=Linger   # expect Linger=yes
```

**4. Confirm the postgres UID in the database image.** `mango-db.container` sets
`UserNS=keep-id:uid=1000,gid=1000` on the assumption it is 1000.

```bash
podman run --rm docker.io/timescale/timescaledb-ha:pg16 id
```

If it differs, edit both numbers in `mango-db.container` to match.

## Installation

Two separate directories are in play, so both are named explicitly rather than
relying on the working directory:

- `BUNDLE` — this directory, containing `containers-systemd/`, `systemd-user/`
  and `config/`. Those subdirectory names exist only to keep the two
  destination trees separate inside one archive; podman does not care about
  them. What matters is where the files land.
- `REPO` — the masschange source checkout containing the `Dockerfile`.

```bash
BUNDLE="$(cd "$(dirname "$0")" && pwd)"   # or just: BUNDLE=/path/to/mango-quadlet
REPO=/path/to/masschange-repo

# 1. Build the image AS THE SERVICE USER. Rootless and rootful podman have
#    separate image stores; an image built with sudo is invisible to these units.
podman build -t localhost/masschange:1.0.0 "$REPO"

# 2. Create the database password secret.
printf '%s' 'pretendpassword' | podman secret create mango-db-password -
podman secret ls

# 3. Environment file.
mkdir -p ~/.config/mango
cp "$BUNDLE/config/mango.env" ~/.config/mango/mango.env
chmod 600 ~/.config/mango/mango.env

# 4. Quadlet units. This destination path is fixed: it is where Quadlet's
#    systemd generator looks for rootless unit definitions.
mkdir -p ~/.config/containers/systemd
cp "$BUNDLE"/containers-systemd/* ~/.config/containers/systemd/

# 5. The hand-written worker target. Different destination, because this is an
#    ordinary systemd unit rather than a Quadlet source file - and that is
#    precisely why it can be `systemctl enable`d when the generated ones cannot.
mkdir -p ~/.config/systemd/user
cp "$BUNDLE/systemd-user/mango-workers.target" ~/.config/systemd/user/

# 6. Generate the systemd units.
systemctl --user daemon-reload

# 7. Confirm all eleven files arrived where they belong.
ls ~/.config/containers/systemd/ ~/.config/systemd/user/mango-workers.target
```

## Verify the generated units before starting anything

This is the single most useful podman command you do not have a Docker
equivalent for. It prints the `.service` files Quadlet produced, including the
full `podman run` command line, without starting anything:

```bash
/usr/libexec/podman/quadlet -user -dryrun
```

Check specifically that `%i` expanded in the worker templates and that every
`-v` path is correct and literal.

## Start

```bash
# Non-template units already have [Install] sections; Quadlet enabled them at
# daemon-reload. Do NOT run `systemctl --user enable` on them - it fails with
# "unit is transient or generated". That is expected, not a mistake.

systemctl --user enable --now mango-workers.target   # this one IS a real file
systemctl --user start mango-api.service mango-crawler.service

systemctl --user list-units 'mango*'
```

Ordering is handled by systemd: `mango-db` holds in `activating` until
`pg_isready` passes, `mango-ensure` runs to completion, everything else follows.

## Operating it: docker to podman

| Task | Docker Compose | Here |
|---|---|---|
| Start everything | `docker compose up -d` | `systemctl --user start mango-workers.target mango-api mango-crawler` |
| Stop everything | `docker compose down` | `systemctl --user stop 'mango-*'` |
| Status | `docker compose ps` | `systemctl --user list-units 'mango*'` or `podman ps` |
| Logs, one service | `docker compose logs -f mango-api` | `journalctl --user -u mango-api -f` |
| Logs, everything | `docker compose logs -f` | `journalctl --user -u 'mango-*' -f` |
| Restart one service | `docker compose restart mango-api` | `systemctl --user restart mango-api` |
| Run the metadata job | `docker compose --profile regenerate-metadata up` | `systemctl --user start mango-regenerate-metadata` |
| Shell into a container | `docker compose exec mango-api bash` | `podman exec -it mango-api bash` |
| Scale workers | edit `replicas:`, `up -d` | edit `mango-workers.target`, `daemon-reload`, `restart mango-workers.target` |

Editing a `.container` file requires `systemctl --user daemon-reload` before
`restart` — the file is a source for a generator, not the unit itself.

## Deploying a new version

Deliberately manual, because your Dockerfile is unpinned (`Miniforge3-latest`,
`yum -y update`, `epel-release-latest`) and any rebuild is non-deterministic. A
`.build` Quadlet unit would move that non-determinism into the boot path.

```bash
podman build -t localhost/masschange:1.1.0 .
sed -i 's|masschange:1.0.0|masschange:1.1.0|' ~/.config/containers/systemd/*.container
systemctl --user daemon-reload
systemctl --user restart mango-api mango-crawler mango-workers.target
```

Rollback is the same `sed` in reverse. The old image is still in local storage.

## Known issues carried over, in priority order

**1. `tsdb-persist` is inside `DATA_ROOT`.** The crawler and all eight workers
mount `/soft/mango` wholesale (necessary so `os.rename()` works across input and
staging), which gives them writable access to the live PostgreSQL data directory
at `/data/tsdb-persist`. The crawler runs with `--remove-src-files`. Fix by
moving `tsdb-persist` and `logs` to siblings of the data tree rather than
children, then updating `DATA_ROOT` and the `Volume=` lines.

**2. TLS paths are placeholders.** Both `Volume=` lines in `mango-api.container`
say `/PLACEHOLDER/ssl/`. The files must be readable by the rootless service user;
the current `/etc/docker/ssl.key/` location almost certainly is not.

**3. Verify `conda run` forwards SIGTERM.** `entrypoint.sh` `exec`s `conda run`,
which makes it PID 1, but `conda run` spawns the application as a child and has
historically not proxied signals. Test:

```bash
podman run -d --name sigtest localhost/masschange:1.0.0 \
  python /app/masschange/src/masschange/ingest/executor/main.py --loop
time podman stop sigtest
```

Under ~2s means signals propagate. ~10s means podman timed out and sent SIGKILL,
and every `systemctl stop` will kill a worker mid-ingest. The fix is to drop
`conda run` and `exec "$@"` with the environment's `bin` prepended to `PATH`.

**4. PostgreSQL on NFS.** `/soft/mango/tsdb-persist` is NFS-backed. Upstream
PostgreSQL considers this hazardous (fsync and locking semantics). Pre-existing,
not migration-induced, but worth revisiting.

**5. Unused variables.** `LOGS_ROOT` was dropped (referenced nowhere in the
compose file). `OFFRED_AGGREGATE` was carried over pending confirmation that
something reads it.

**6. `mango-ensure` and `mango-regenerate-metadata` receive `DATA_ROOT`** from
the shared environment file and will create dangling `/input` and `/staging`
symlinks. Harmless unless those scripts ever dereference them; splitting the
environment file would remove the smell.

**7. If SELinux is ever set to enforcing**, every `Volume=` line will need a `:z`
suffix (shared relabel, not `:Z`) — and relabeling is not possible on NFS, so
you would need `setsebool -P container_use_nfs on` instead.
