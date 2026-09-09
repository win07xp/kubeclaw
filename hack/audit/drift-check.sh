#!/usr/bin/env bash
# drift-check.sh: report which #139 audit verdicts need a manual re-walk.
#
# Reads hack/audit/manifest.txt and diffs each verdict's paths from the
# audited commit to HEAD. An empty diff means the verdict stands and needs
# no audit work; a non-empty one names the verdict and the changed files.
# Exits 1 when any verdict drifted, so a release runs it as a gate; CI runs
# it non-blocking, because drift between audits is expected and the answer
# is a targeted re-walk before the next release, not a red build.
set -euo pipefail

manifest="$(dirname "$0")/manifest.txt"
audited=$(sed -n 's/^# audited-commit: //p' "$manifest")
if [ -z "$audited" ]; then
    echo "manifest carries no audited-commit line" >&2
    exit 2
fi
if ! git rev-parse --verify --quiet "$audited^{commit}" > /dev/null; then
    echo "audited commit $audited is not in this clone" >&2
    exit 2
fi

drifted=0
while read -r verdict paths; do
    case "$verdict" in ""|\#*) continue ;; esac
    # shellcheck disable=SC2086
    changed=$(git diff --name-only "$audited"..HEAD -- $paths)
    if [ -n "$changed" ]; then
        drifted=1
        echo "verdict $verdict drifted since $audited:"
        echo "$changed" | sed 's/^/    /'
    fi
done < "$manifest"

if [ "$drifted" -eq 0 ]; then
    echo "no drift: every audited path is unchanged since $audited; all verdicts stand"
else
    echo "re-walk the drifted verdicts and re-stamp the manifest before the next release"
fi
exit "$drifted"
