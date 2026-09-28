#!/usr/bin/env bash
# install.sh — install the gomailer daemon as a systemd unit.
#
# Installs the PRE-BUILT ./gomailer binary plus the unit and sysusers
# conf. It involves no Go toolchain and DOWNLOADS NOTHING — build first
# on a machine with the toolchain (README "Build"):
#
#   go build -o gomailer .
#
# then, from the repository root, the RUNBOOK §2 sequence in one command:
#
#   install -m755  gomailer              -> $BIN_DIR/gomailer
#   install -m644  gomailer.service      -> $UNIT_DIR/gomailer.service
#   install -D -m644 gomailer.sysusers.conf -> $SYSUSERS_DIR/gomailer.conf
#   systemd-sysusers                      (idempotent; creates the user)
#   systemctl daemon-reload && enable && (start | restart)
#
# Idempotent: safe to re-run — and the re-run IS the upgrade path (build
# the new binary, re-run: a running daemon is restarted, which drains
# in-flight sends gracefully per the unit's stop discipline). The unit's
# ExecStart hardcodes /usr/local/bin/gomailer, so the default BIN_DIR is
# part of the contract. Overriding the *_DIR variables is the
# testing/packaging seam (it also lifts the root requirement).
#
# Usage: sudo ./install.sh
set -euo pipefail

BIN_DIR=${BIN_DIR:-/usr/local/bin}
UNIT_DIR=${UNIT_DIR:-/etc/systemd/system}
SYSUSERS_DIR=${SYSUSERS_DIR:-/etc/sysusers.d}

say() { printf 'install.sh: %s\n' "$*"; }
die() { printf 'install.sh: ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
	cat <<'EOF'
install.sh — install the gomailer daemon as a systemd unit.

Usage: sudo ./install.sh

Installs the pre-built ./gomailer binary (build it first:
go build -o gomailer .) plus the unit and sysusers conf, creates the
service user, enables and starts the daemon. No Go toolchain, no
network, nothing downloaded.

Idempotent and safe to re-run: the re-run is the upgrade path (a
running daemon is restarted — graceful, in-flight sends drain first).

Environment overrides (testing/packaging): BIN_DIR, UNIT_DIR,
SYSUSERS_DIR — overriding all three also lifts the root requirement.
EOF
}

case "${1:-}" in
-h|--help) usage; exit 0 ;;
"") ;;
*) die "unknown argument: $1 (see --help)" ;;
esac

# Root by default — the destinations and systemctl need it. Overriding
# every destination opts out (the dry-layout seam used by tests).
if [[ $EUID -ne 0
	&& $BIN_DIR == /usr/local/bin
	&& $UNIT_DIR == /etc/systemd/system
	&& $SYSUSERS_DIR == /etc/sysusers.d ]]; then
	die "must run as root (sudo ./install.sh), or override BIN_DIR/UNIT_DIR/SYSUSERS_DIR for a dry layout"
fi

cd "$(dirname "$0")"
[[ -f gomailer.service && -f gomailer.sysusers.conf ]] ||
	die "run from the repository (gomailer.service/gomailer.sysusers.conf not found)"
[[ -x ./gomailer ]] ||
	die "no ./gomailer binary — build it first (go build -o gomailer .); the installer downloads and builds nothing"

if [[ $BIN_DIR != /usr/local/bin ]]; then
	say "WARNING: BIN_DIR=$BIN_DIR, but the unit's ExecStart is /usr/local/bin/gomailer — the service will not find the binary"
fi

say "installing binary -> $BIN_DIR/gomailer"
install -m755 gomailer "$BIN_DIR/gomailer"
say "installing unit -> $UNIT_DIR/gomailer.service"
install -m644 gomailer.service "$UNIT_DIR/gomailer.service"
say "installing sysusers conf -> $SYSUSERS_DIR/gomailer.conf"
install -D -m644 gomailer.sysusers.conf "$SYSUSERS_DIR/gomailer.conf"

# Creates the gomailer user/group only when missing (idempotent), then
# make systemd see the (possibly updated) unit and bring it up.
systemd-sysusers
systemctl daemon-reload
systemctl enable gomailer

if systemctl is-active --quiet gomailer; then
	say "daemon running — restarting to pick up the new binary (graceful: in-flight sends drain, bounded by TimeoutStopSec=90s)"
	systemctl restart gomailer
else
	say "starting the daemon"
	systemctl start gomailer
fi

say "done. Health check:            gomailer -ping"
say "Schedule a mail (as the service user — the store and socket are owner-only):"
say "  sudo -u gomailer env PEC_PASSWORD=... gomailer -at \"15:00:00\" -to dest@pec.it -subject S -body B"
say "Inspect the store:            sudo -u gomailer gomailer -list"
say "Uninstall (keeps the evidence store by default):  sudo ./uninstall.sh"