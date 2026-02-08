# Claude Instructions

## Required Reading

Always read the `AGENTS.md` file in the same directory as this file before starting any task. It contains important context about the codebase and development practices.

## Interaction Style

**Answer First, Then Wait**: When the user asks a question, provide an answer and then STOP. Wait for user input before taking any action. Do not immediately start implementing changes based on your interpretation of the question.

**Never Jump to Implementation**: After answering a question or proposing a solution, always wait for explicit user confirmation before writing any code or making changes. This applies to:
- Bug fixes
- New features
- Refactoring
- Test implementations
- Any code modifications

**Design Confirmation Required**: When proposing a fix or new implementation approach, always wait for explicit user confirmation that the design is acceptable before writing any code. Never jump to implementation based on your own analysis.

**CRITICAL - Always Get Confirmation Before Implementing**: When discussing a change or issue with the user:
1. First, explain your understanding of the problem
2. Propose the design/approach you would take
3. STOP and explicitly ask: "Does this approach look good to you?" or similar
4. Wait for the user to confirm before writing ANY code
5. If the user asks a clarifying question, answer it and wait again

This rule applies even when the path forward seems obvious. The user may have context or preferences you're not aware of.

**No Unilateral Design Changes**: When encountering technical challenges during implementation that require changing the approved design (e.g., changing from suspend to non-suspend functions, switching from one API style to another), STOP and ask the user for confirmation before proceeding. Never make significant design changes on your own, even if they seem technically necessary. Present the problem and proposed alternatives, then wait for the user to decide.

## Test Integrity

**Never Modify Tests to Hide Bugs**: When a test fails, the problem is usually in the code, not the test. Never remove, weaken, or modify tests to make them pass when there's a real issue in the implementation. Instead:
- Investigate the root cause of the failure
- Fix the actual code issue
- Only modify a test if it is genuinely incorrect or testing the wrong behavior

**Ask Before Modifying Tests**: When investigating test execution problems and you believe the solution requires modifying a test, always ask the user and wait for explicit confirmation before changing the test. Explain why you think the test needs to change.

## Committing

**Run Tests Before Committing**: Never create a commit before verifying that unit tests pass. Run `./gradlew :temporal-sdk:test :temporal-kotlin:test` (or the relevant module tests) and confirm they pass before committing.

## GitHub PR Reviews

**Include Comment IDs When Saving PR Comments**: When downloading or saving PR review comments to a file for later processing, always include the comment ID. This allows replying to specific comments later:

```bash
gh api repos/{owner}/{repo}/pulls/{pr}/comments | jq '.[] | {id: .id, user: .user.login, body: .body, path: .path}'
```

**Reply to Specific Comments by ID**: When responding to PR review comments, always reply to the specific comment by its ID using the GitHub API:

```bash
gh api repos/{owner}/{repo}/pulls/{pr}/comments/{comment_id}/replies -f body="Your response"
```

This ensures responses are threaded correctly under the original comment rather than appearing as new top-level comments.

## Temporal Service Health Checks

**Use Temporal SDK, Not curl**: When checking if the Temporal service is running, use the Temporal CLI or SDK commands instead of curl. The Temporal server doesn't expose a simple HTTP health endpoint.

```bash
# Correct: Use temporal CLI
temporal operator namespace list

# Incorrect: Don't use curl
# curl http://localhost:7233/health  # This won't work
```
