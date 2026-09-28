#!/usr/bin/env bash
# uninstall.sh — remove the gomailer daemon (the reverse of install.sh).
#
# The state directory ($STATE_DIR, default /var/lib/gomailer) holds the
# payload originals of the jobs that have not yet fired (they are
# released at settle), the permanent row history (the audit log), and
# the mailbox credentials of pending jobs. Whether anything in it is
# your conservation copy is YOUR policy — conserving the originals for
# legal purposes is the sender's duty (DM 2/11/2005), not gomailer's.
# The directory is therefore KEPT by default — removal is a separate,
# explicitly confirmed act.
#
# The gomailer system user/group (created by systemd-sysusers) and
# /etc/gomailer.env are left in place: an unused system account is
# harmless, and deleting accounts is riskier than leaving them. The
# script touches only absolute paths, so it works with the repository
# already deleted.
#
# Usage: sudo ./uninstall.sh [--purge-state] [--yes]
set -euo pipefail

BIN_DIR=${BIN_DIR:-/usr/local/bin}
UNIT_DIR=${UNIT_DIR:-/etc/systemd/system}
SYSUSERS_DIR=${SYSUSERS_DIR:-/etc/sysusers.d}
STATE_DIR=${STATE_DIR:-/var/lib/gomailer}

say() { printf 'uninstall.sh: %s\n' "$*"; }
die() { printf 'uninstall.sh: ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
	cat <<'EOF'
uninstall.sh — remove the gomailer daemon (the reverse of install.sh).

Usage: sudo ./uninstall.sh [--purge-state] [--yes]

  (default)     stop + disable the unit, remove the binary, the unit and
                the sysusers conf. The state dir is KEPT — it holds the
                payload originals of unfired jobs, the permanent row
                history and the credentials of jobs still pending.
                Whether it is your conservation copy is your policy
                (conserving originals is the sender's duty, not
                gomailer's).
  --purge-state also delete the state dir. Asks for confirmation;
                combine with --yes to confirm non-interactively.
  --yes         answer the purge confirmation without asking.

Environment overrides (testing/packaging): BIN_DIR, UNIT_DIR,
SYSUSERS_DIR, STATE_DIR — overriding all four defaults also lifts the
root requirement.
EOF
}

purge_state=0
assume_yes=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--purge-state) purge_state=1 ;;
	--yes) assume_yes=1 ;;
	-h|--help) usage; exit 0 ;;
	*) die "unknown flag: $1 (see --help)" ;;
	esac
	shift
done

if [[ $EUID -ne 0
	&& $BIN_DIR == /usr/local/bin
	&& $UNIT_DIR == /etc/systemd/system
	&& $SYSUSERS_DIR == /etc/sysusers.d
	&& $STATE_DIR == /var/lib/gomailer ]]; then
	die "must run as root (sudo ./uninstall.sh), or override the *_DIR/STATE_DIR variables for a dry layout"
fi

# Stop FIRST, while the unit is still loaded: the stop is graceful by
# design (TimeoutStopSec=90s), so a runner mid-send finishes its commit —
# a SIGKILL here would turn sends into crash-ambiguity cases (RUNBOOK §7).
say "stopping the daemon (graceful: in-flight sends drain)"
systemctl stop gomailer 2>/dev/null || say "unit not loaded — nothing to stop"
systemctl disable gomailer 2>/dev/null || true
systemctl reset-failed gomailer 2>/dev/null || true

rm_one() {
	if [[ -e "$1" ]]; then
		say "removing $1"
		rm -f "$1"
	else
		say "$1 not present — nothing to remove"
	fi
}
rm_one "$BIN_DIR/gomailer"
rm_one "$UNIT_DIR/gomailer.service"
rm_one "$SYSUSERS_DIR/gomailer.conf"
systemctl daemon-reload

if [[ $purge_state -eq 1 ]]; then
	if [[ ! -e $STATE_DIR ]]; then
		say "state dir $STATE_DIR not present — nothing to purge"
	elif [[ $assume_yes -eq 1 ]]; then
		say "--purge-state --yes: deleting $STATE_DIR (index, payload blobs, lock sidecars)"
		rm -rf "$STATE_DIR"
		say "state deleted"
	elif [[ -t 0 ]]; then
		say "$STATE_DIR holds the payload originals of jobs not yet fired, the permanent row history, and pending credentials."
		say "Whether it is your conservation copy is YOUR policy (conserving originals is the sender's duty — DM 2/11/2005)."
		say "Archive it first if anything might ever be disputed:"
		say "  tar -C $(dirname "$STATE_DIR") -czf gomailer-evidence-$(date +%F).tgz $(basename "$STATE_DIR")"
		printf 'Type PURGE to delete it permanently: '
		IFS= read -r answer
		if [[ $answer == PURGE ]]; then
			rm -rf "$STATE_DIR"
			say "state deleted"
		else
			die "not confirmed — state kept (rerun with --purge-state when sure)"
		fi
	else
		die "refusing to purge $STATE_DIR without a terminal (rerun with --purge-state --yes, after archiving)"
	fi
else
	if [[ -e $STATE_DIR ]]; then
		say "state KEPT at $STATE_DIR — the payload originals of unfired jobs, the permanent row history, and the"
		say "credentials of jobs still pending. Whether it is your conservation copy is your policy;"
		say "conserving the originals for legal purposes is the sender's duty, not gomailer's. Archive it before"
		say "wiping this machine:"
		say "  tar -C $(dirname "$STATE_DIR") -czf gomailer-evidence-$(date +%F).tgz $(basename "$STATE_DIR")"
		say "Remove it explicitly when you are sure:  sudo ./uninstall.sh --purge-state"
	else
		say "no state dir at $STATE_DIR — nothing kept, nothing to archive"
	fi
fi

say "left in place: the gomailer system user/group and /etc/gomailer.env (if present)"
say "uninstalled"