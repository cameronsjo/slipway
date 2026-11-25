#!/usr/bin/env bash
# The controller verifies this Pod's downward API identity before sending the
# one-use capability. The capability is never written to disk or argv.
set -euo pipefail

[[ $# -gt 0 ]] || { echo 'runner-pool bootstrap requires worker arguments' >&2; exit 2; }
[[ $(id -u) == 1001 && $(id -g) == 121 ]] || { echo 'runner-pool worker must be UID1001:GID121' >&2; exit 1; }

name=${SLIPWAY_BOOTSTRAP_EXPECTED_NAME:?}
uid=${SLIPWAY_BOOTSTRAP_EXPECTED_UID:?}

# THE CONTROLLER SPEAKS FIRST. This used to print HELLO here, at startup, and
# then block on read -- but the controller attaches roughly ten seconds later,
# after the Pod is Ready and the physical proof is taken, and A KUBERNETES
# ATTACH DOES NOT REPLAY OUTPUT EMITTED BEFORE THE ATTACH. Proven on the
# cluster: a throwaway Kata pod's startup line appeared in `kubectl logs` and
# never on the attach stream. The controller waited forever for a line already
# said, and every worker creation failed.
#
# So the identity is announced only when asked. `read` returning non-zero is
# EOF or a short read, and under `set -e` a bare `read` would abort the script
# with no message at all -- which on this path is indistinguishable from a
# worker that simply never spoke. Both are handled explicitly.
if ! IFS= read -r prompt; then
    echo 'runner-pool bootstrap stdin closed before the identity prompt' >&2
    exit 1
fi
readonly max_frame=256
[[ ${#prompt} -le $max_frame ]] || { echo 'runner-pool bootstrap prompt exceeds the frame bound' >&2; exit 1; }
# Exactly HELLO?, compared whole. Anything else exits WITHOUT printing the
# identity and without echoing what arrived: a worker that repeated its input
# would turn this stream into a reflector, and one that announced itself to an
# unexpected opener would answer a question nobody with the capability asked.
[[ "$prompt" == 'HELLO?' ]] || { echo 'runner-pool bootstrap expected the identity prompt' >&2; exit 1; }
unset prompt
printf 'HELLO %s %s\n' "$name" "$uid"

if ! IFS= read -r frame; then
    echo 'runner-pool bootstrap stdin closed before the capability frame' >&2
    exit 1
fi
[[ ${#frame} -le $max_frame ]] || { echo 'runner-pool bootstrap capability frame exceeds the bound' >&2; exit 1; }
case "$frame" in
    'CAP '*) token=${frame#CAP } ;;
    *) echo 'runner-pool bootstrap expected capability frame' >&2; exit 1 ;;
esac
unset frame

[[ ${#token} == 43 ]] || { echo 'runner-pool bootstrap capability has the wrong length' >&2; exit 1; }
case "$token" in
    *[!A-Za-z0-9_-]*) echo 'runner-pool bootstrap capability is not base64url' >&2; exit 1 ;;
esac
export SLIPWAY_BOOTSTRAP_TOKEN="$token"
unset token
printf 'ACK %s %s\n' "$name" "$uid"
unset name uid SLIPWAY_BOOTSTRAP_EXPECTED_NAME SLIPWAY_BOOTSTRAP_EXPECTED_UID
exec "$@"
