---
name: pr-workflow
description: Use for working with GitHub pull requests — querying review threads, resolving them via GraphQL, adding comment replies, managing PR status. Use when the user asks to resolve, query, update, or work with PR review comments, threads, or reviews. Follow strict one-at-a-time workflow: present plan, wait for approval, implement, get confirmation, then move to next.
---

# PR Workflow Skill

Work with GitHub pull requests using `gh` CLI and GitHub REST/GraphQL APIs.

## Prerequisites

- SSH key for git operations: `~/.ssh/id_ed25519_git` (passwordless)
- `gh` CLI authenticated with `gh auth login`

### Git SSH setup

```bash
export SSH_AUTH_SOCK="/tmp/opencode-ssh-agent.sock"
export GIT_SSH_COMMAND="ssh -i ~/.ssh/id_ed25519_git -o StrictHostKeyChecking=no -F /dev/null"
```

## GitHub API Request Patterns

### Using curl with GraphQL

Write the JSON payload to a temp file, then pass it with `--data-binary`:

```bash
cat > /tmp/pr_query.json << 'ENDJSON'
{
  "query": "query { r: repository(owner:\"OWNER\", name:\"REPO\") { pr: pullRequest(number:N) { threads: reviewThreads(first:50) { nodes { id isResolved cmts: comments(first:10) { nodes { id body } } } } } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/pr_query.json 2>&1 | python3 -m json.tool
```

**Important rules for GraphQL queries:**
- **Use aliases on root fields** to avoid parsing conflicts: `repository` → `r`, `pullRequest` → `pr`, `reviewThreads` → `threads`
- **`comments` inside `reviewThread` requires pagination**: `comments(first:10) { nodes { ... } }` (NOT `comments { nodes { ... } }`)
- **Alias `comments`** to avoid parser issues: `cmts: comments(first:10)`
- Write queries to temp files (`cat > /tmp/query.json << 'ENDJSON'`) to avoid shell escaping issues with nested quotes
- Parse output with `python3 -m json.tool` for readability

### Using `gh` CLI (REST API)

```bash
# List all review comments on a PR
gh api repos/OWNER/REPO/pulls/N/comments --jq '.[] | {id, body, path, line, resolved}'

# List review comments for a specific file/line
gh api repos/OWNER/REPO/pulls/N/comments -f path=FILEPATH -f line=LINE_NUMBER 2>/dev/null

# Get PR details
gh pr view N --json title,state,files,additions,deletions,reviewComments,commits
```

### Parsing JSON with Python

```bash
# Convert JSON output to readable format
curl ... 2>&1 | python3 -m json.tool

# Extract specific fields
curl ... 2>&1 | python3 -c "
import json, sys
data = json.load(sys.stdin)
if 'errors' in data:
    print('ERROR:', data['errors'])
    sys.exit()
threads = data['data']['r']['pr']['threads']['nodes']
for t in threads:
    status = 'RESOLVED' if t['isResolved'] else 'UNRESOLVED'
    print(f'{status} {t[\"id\"]}')
    for c in t['cmts']['nodes']:
        print(f'  Body: {c[\"body\"][:200]}')
"
```

## Querying PR Review Threads

### Via GraphQL (recommended — has `isResolved` field)

```bash
cat > /tmp/pr_query.json << 'ENDJSON'
{
  "query": "query { r: repository(owner:\"OWNER\", name:\"REPO\") { pr: pullRequest(number:N) { threads: reviewThreads(first:50) { nodes { id isResolved cmts: comments(first:10) { nodes { id body } } } } } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/pr_query.json 2>&1 | python3 -m json.tool
```

Key fields from thread nodes:
- `id` — thread ID (used in `resolveReviewThread` mutation)
- `isResolved` — boolean
- `cmts.nodes[].id` — comment ID
- `cmts.nodes[].body` — comment text

### Via REST API (no `isResolved` field)

```bash
gh api repos/OWNER/REPO/pulls/N/comments --jq '.[] | {id, body, path, line, resolved}'
```

REST API only returns comments, not the resolved state. Use GraphQL for state queries.

## Resolving Review Threads

### Via GraphQL

```bash
cat > /tmp/resolve.json << 'ENDJSON'
{
  "query": "mutation { resolveReviewThread(input: { threadId: \"THREAD_ID\" }) { thread { id isResolved } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/resolve.json 2>&1 | python3 -m json.tool
```

The `resolveReviewThread` mutation sets a thread's `isResolved` flag to `true`.

### Via REST API (deprecated)

```bash
gh api repos/OWNER/REPO/pulls/comments/COMMENT_ID/resolved -X PUT -f resolved=true 2>/dev/null
```

REST API marks individual comments as resolved, not the entire thread. Use GraphQL for proper thread resolution.

## Adding Comment Replies

### Via REST API

```bash
gh api repos/OWNER/REPO/pulls/comments/COMMENT_ID/reviews \
  -X POST -F body="RESPONSE_TEXT" 2>/dev/null
```

Or using curl:

```bash
cat > /tmp/reply.json << 'ENDJSON'
{
  "body": "RESPONSE_TEXT"
}
ENDJSON
curl -s -X POST "https://api.github.com/repos/OWNER/REPO/pulls/comments/COMMENT_ID/reviews" \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/reply.json 2>/dev/null
```

## PR Operations

### Get PR info

```bash
gh pr view N --json title,state,files,additions,deletions,reviewComments,commits
```

### Get PR diff

```bash
gh pr diff N
```

### Get PR commits

```bash
gh pr commits N
```

### List PR review threads

```bash
gh api repos/OWNER/REPO/pulls/N/comments --jq '.[] | {path, line, body}'
```

## Workflow Rules (STRICT)

### Approval Required

- **NEVER** close/resolve comments or conversations without explicit user approval.
- **NEVER** implement fixes immediately after reading comments.
- **NEVER** present plans for multiple comments at once.
- **NEVER** ask for confirmation during Phase 1 — only present plans and move to the next comment.
- **NEVER** ask "Can this conversation be resolved?" until Phase 3 (after all implementations are done).
- **NEVER** move to the next comment until the current one is fully resolved.

### Test Verification Rule

**NEVER assume infrastructure is broken when tests fail after your changes.**
- **ALWAYS verify** that tests were green BEFORE your changes (stash your changes, build, run tests).
- **If tests fail after your changes** and were green before → your changes broke something. Investigate your code, not the infrastructure.
- **If tests fail both before and after your changes** → pre-existing infrastructure issue. Report it but don't let it block your work.
- This rule prevents wasting time debugging non-existent regressions and ensures all PR changes are backed by green tests.

### Comment Resolution Rule

When the user says a comment is "resolved", immediately resolve the corresponding thread on GitHub via GraphQL. Do not wait for multiple comments to be resolved at once. Resolve each thread as soon as the user confirms it.

### Required Workflow (ONE AT A TIME)

**Phase 1 — Collect Plans**
1. **Query** all unresolved PR review threads via GraphQL.
2. **Present ONE comment** (chronological order) with its implementation plan.
3. **Remember the plan** but do NOT implement yet.
4. **WAIT for user to say "next" or "continue"** before moving on.
5. **Only after user says "next":** present the next comment — repeat steps 2-5 until ALL comments are presented.

**Phase 2 — Implement All**
5. **Implement ALL approved plans** in order.
6. **Rebuild all** — `dotnet build src/nKafka.sln`.
7. **Run ALL tests** — `dotnet test`.
8. **Run benchmarks** — all benchmark suites.
9. **Update MD files** — README.md, benchmark.md, etc.
10. **Commit and push** all changes.

**Phase 3 — Confirm One by One**
11. **Report for each comment:** show what was asked + how it was fixed.
12. **Ask for confirmation:** "Can this conversation be resolved?"
13. **If YES:** reply to the comment with resolution note, resolve the thread via GraphQL `resolveReviewThread`.
14. **If NO:** prepare implementation plan, show it to user, wait for confirmation.
15. **Move to the next comment** — repeat steps 11-14 until all comments are resolved.
8. Continue until all comments are resolved.

## Common Patterns

### Workflow for resolving comments (one at a time)

**Phase 1 — Collect Plans:**
1. Query PR review threads via GraphQL to get all threads with `isResolved` state.
2. Present ONE comment (chronological order) with its implementation plan.
3. Remember the plan — do NOT implement yet.
4. Move to the next comment — repeat steps 2-3 until ALL comments are presented.

**Phase 2 — Implement All:**
5. Implement ALL approved plans at once.
6. Rebuild all, run ALL tests, run benchmarks, update MD files, commit and push.

**Phase 3 — Confirm One by One:**
7. Report for each comment: what was asked + how it was fixed.
8. Ask for confirmation: "Can this conversation be resolved?"
9. If YES → reply + resolve thread via GraphQL. If NO → prepare plan, wait for confirmation.
10. Move to next comment — repeat steps 7-9.

### Bulk resolving multiple threads

All `resolveReviewThread` mutations can be batched in a single GraphQL request:

```bash
cat > /tmp/batch_resolve.json << 'ENDJSON'
{
  "query": "mutation { r1: resolveReviewThread(input: { threadId: \"ID1\" }) { thread { id isResolved } } r2: resolveReviewThread(input: { threadId: \"ID2\" }) { thread { id isResolved } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/batch_resolve.json 2>&1 | python3 -m json.tool
```

### Pagination for large PRs

When a PR has many threads, paginate:

```bash
cat > /tmp/paginated.json << 'ENDJSON'
{
  "query": "query { r: repository(owner:\"OWNER\", name:\"REPO\") { pr: pullRequest(number:N) { threads: reviewThreads(first:20 after:\"CURSOR\") { totalCount page { hasNextPage } nodes { id isResolved } } } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/paginated.json 2>&1 | python3 -m json.tool
```

### Resolving a thread after user says "comment resolved"

When the user confirms a comment is resolved:

```bash
cat > /tmp/resolve.json << 'ENDJSON'
{
  "query": "mutation { resolveReviewThread(input: { threadId: \"THREAD_ID\" }) { thread { id isResolved } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/resolve.json 2>&1
```

Then verify:
```bash
cat > /tmp/verify.json << 'ENDJSON'
{
  "query": "query { r: repository(owner:\"OWNER\", name:\"REPO\") { pr: pullRequest(number:N) { threads: reviewThreads(first:50) { nodes { id isResolved } } } } }"
}
ENDJSON
curl -s -X POST https://api.github.com/graphql \
  -H "Authorization: Bearer $(gh auth token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/verify.json 2>&1 | python3 -c "
import json, sys
data = json.load(sys.stdin)
threads = data['data']['r']['pr']['threads']['nodes']
resolved = sum(1 for t in threads if t['isResolved'])
unresolved = sum(1 for t in threads if not t['isResolved'])
print(f'Total: {len(threads)}, Resolved: {resolved}, Unresolved: {unresolved}')
"
```
