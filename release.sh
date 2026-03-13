#!/bin/bash
set -euo pipefail

BINARY="goofys"

# Require a version tag as argument
if [ $# -lt 1 ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 v0.24.0"
    exit 1
fi

VERSION="$1"

# Verify gh CLI is available
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is required. Install it from https://cli.github.com/"
    exit 1
fi

# Verify we're in a clean git state
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working directory is not clean. Commit or stash changes first."
    exit 1
fi

# Detect the current branch and its tracking remote
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
REMOTE="$(git config "branch.${BRANCH}.remote" 2>/dev/null || true)"

if [ -z "${REMOTE}" ]; then
    echo "Error: Branch '${BRANCH}' does not track a remote."
    echo "Set one with: git branch --set-upstream-to=<remote>/${BRANCH}"
    exit 1
fi

# Extract owner/repo from the remote URL
REMOTE_URL="$(git remote get-url "${REMOTE}")"
# Handle both SSH (git@github.com:owner/repo.git) and HTTPS (https://github.com/owner/repo.git)
REPO="$(echo "${REMOTE_URL}" | sed -E 's|^.*github\.com[:/]||; s|\.git$||')"

if [ -z "${REPO}" ]; then
    echo "Error: Could not parse GitHub owner/repo from remote URL: ${REMOTE_URL}"
    exit 1
fi

echo "Branch: ${BRANCH}"
echo "Remote: ${REMOTE} (${REMOTE_URL})"
echo "Repo:   ${REPO}"
echo ""

# Create and push the tag
echo "Creating tag ${VERSION}..."
git tag -a "${VERSION}" -m "Release ${VERSION}"
git push "${REMOTE}" "${VERSION}"

# Build binaries
echo "Building binaries..."
make build-all

# Create the GitHub release with both binaries
echo "Creating GitHub release ${VERSION}..."
gh release create "${VERSION}" \
    --repo "${REPO}" \
    --title "${VERSION}" \
    --generate-notes \
    "${BINARY}-linux-amd64#${BINARY}-linux-amd64" \
    "${BINARY}-linux-arm64#${BINARY}-linux-arm64"

echo "Release ${VERSION} published: https://github.com/${REPO}/releases/tag/${VERSION}"
