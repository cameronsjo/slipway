#!/bin/bash
# tart-pf-assert.sh — idempotent. Guarantees the tart-ci boundary is IN FORCE,
# where "in force" means four things, not one.
set -uo pipefail
ANCHOR=tart-ci
CONF=/etc/pf-tart.conf
AFILE=/etc/pf.anchors/tart-ci
# SHA-256 of the repo's hosts/m1max/pf/tart-ci.anchor. Documented in that
# directory's README.md § Anchor hash, which is the source of truth an operator
# checks against — this constant and that section change together.
EXPECT_HASH=a029bf64786c92c52b938ac33551c42a8904984fa5281db12f7b0916e86c31ae
VM=ci-guest
RUNAS=cameron
LOG=/var/log/tart-pf-assert.log
exec >>"$LOG" 2>&1
echo "--- $(date '+%F %T') assert ---"

fail(){ echo "ASSERT FAILED: $1"
        # B5: the boundary used to fail closed at start and OPEN at runtime — a
        # failed re-assert logged and exited while a guest kept running with no
        # containment. Stop the guest instead.
        if sudo -u "$RUNAS" -H /opt/homebrew/bin/tart list 2>/dev/null | grep -q "$VM.*running"; then
            echo "  guest $VM is RUNNING with the boundary unverified — stopping it"
            sudo -u "$RUNAS" -H /opt/homebrew/bin/tart stop "$VM" >/dev/null 2>&1
            sleep 3
            sudo -u "$RUNAS" -H /opt/homebrew/bin/tart list 2>/dev/null | grep -q "$VM.*running" \
                && echo "  *** GUEST STILL RUNNING AFTER STOP — MANUAL ACTION NEEDED ***" \
                || echo "  guest stopped"
        fi
        exit 1; }

[ -f "$CONF" ] && [ -f "$AFILE" ] || fail "missing $CONF or $AFILE"

# B4: anchor CONTENT must match what was reviewed, not merely parse.
GOT_HASH=$(shasum -a 256 "$AFILE" | awk '{print $1}')
[ "$GOT_HASH" = "$EXPECT_HASH" ] || fail "anchor file changed (expected ${EXPECT_HASH:0:16}, got ${GOT_HASH:0:16})"

# B4: a discarded exit status here meant a parse failure enabled pf against a
# STALE ruleset and every downstream check still passed.
if ! /sbin/pfctl -f "$CONF" 2>/tmp/pfload.err; then
    fail "pfctl -f failed: $(head -2 /tmp/pfload.err | tr '\n' ' ')"
fi
/sbin/pfctl -E 2>&1 | grep -i 'enabled\|already' || true

ADDRS=$(/sbin/ifconfig | awk '/inet /{print $2}' | grep -v '^127\.' | sort -u | tr '\n' ' ')
if [ -n "$ADDRS" ]; then
    if /sbin/pfctl -a "$ANCHOR" -t hostaddrs -T replace $ADDRS >/dev/null 2>&1; then
        echo "table hostaddrs <- $ADDRS"
    else
        echo "WARNING: hostaddrs replace FAILED — table may be stale"
    fi
fi
# B1: same treatment for v6. Global addresses only — link-local and ULA are
# covered by prefix rules and would bloat the table on every interface.
ADDRS6=$(/sbin/ifconfig | awk '/inet6 /{split($2,a,"%"); print a[1]}' | grep -viE '^fe80|^::1|^f[cd]' | sort -u | tr '\n' ' ')
if [ -n "$ADDRS6" ]; then
    /sbin/pfctl -a "$ANCHOR" -t hostaddrs6 -T replace $ADDRS6 >/dev/null 2>&1 \
        && echo "table hostaddrs6 <- $ADDRS6" || echo "WARNING: hostaddrs6 replace failed"
fi

STATUS=$(/sbin/pfctl -s info 2>/dev/null | head -1)
RULES=$(/sbin/pfctl -a "$ANCHOR" -s rules 2>/dev/null | grep -c 'block drop')
MAIN=$(/sbin/pfctl -s rules 2>/dev/null)
REFERENCED=$(printf '%s' "$MAIN" | grep -c "anchor \"$ANCHOR\"")

# B2: ORDER. An anchor can be loaded AND referenced and still lose, because
# first match wins and internet-sharing passes traffic on its shared network.
# This is the property whose absence already let the guest reach host :22.
POS_OURS=$(printf '%s' "$MAIN" | grep -n "anchor \"$ANCHOR\"" | head -1 | cut -d: -f1)
POS_IS=$(printf '%s' "$MAIN" | grep -n 'anchor "com.apple.internet-sharing' | head -1 | cut -d: -f1)

echo "pf: $STATUS ; block rules: $RULES ; referenced: $REFERENCED ; pos ours=${POS_OURS:-none} is=${POS_IS:-none}"

case "$STATUS" in *Enabled*) ;; *) fail "pf not enabled ($STATUS)";; esac
# 8 since homelab#417 added the bridge100 antispoof block: 4 IPv4 + 1 antispoof
# + 3 IPv6. The hash pin above already catches a changed anchor; this catches a
# ruleset that loaded from somewhere else entirely.
[ "${RULES:-0}" -ge 8 ]      || fail "only ${RULES:-0} block rules (expected >=8)"
[ "${REFERENCED:-0}" -ge 1 ] || fail "anchor loaded but NOT referenced — its rules never evaluate"
if [ -n "${POS_IS:-}" ]; then
    [ -n "${POS_OURS:-}" ] || fail "tart-ci not found in the main ruleset"
    [ "$POS_OURS" -lt "$POS_IS" ] || fail "ORDER: tart-ci (line $POS_OURS) is AFTER internet-sharing (line $POS_IS) — its blocks lose to that anchor's passes"
fi
echo "assert OK"
exit 0
