# Explicit ClawRouter routing

The local Claude CLI and ACP entrypoints accept `clawrouter/<model>` as an explicit provider selection. The adapter removes that prefix for the child process. It does not change the stored model or agent configuration.

Configure these variables on the trusted Paperclip server:

- `PAPERCLIP_CLAWROUTER_BASE_URL`: the ClawRouter HTTP(S) base URL. Do not include credentials, query parameters, or a fragment.
- `CLAWROUTER_PROXY_KEY`: the existing server credential for that router.

The adapter reads those two variables only from the server environment. Agent environment values cannot select the router URL or the credential variable. The child receives the URL as `ANTHROPIC_BASE_URL` and the credential as `ANTHROPIC_AUTH_TOKEN`. An empty `ANTHROPIC_API_KEY` suppresses a competing inherited API key. The credential stays in the child environment; it is not added to command arguments or persisted in agent configuration.

Models without the `clawrouter/` prefix keep their current route and authentication. Explicit ClawRouter selection fails before launch if the server configuration is missing or invalid, if another provider mode or subscription OAuth token is active, or if the execution target is remote. This feature does not export server credentials to remote targets.

The environment test validates this same route configuration. Its existing Claude hello and ACP prerequisite checks remain separate from an actual ACP tool turn. A successful environment test is not proof that an agent's business work completed.

Rollback the application image and remove the non-secret server URL setting to revert the feature. No agent configuration or credential migration is required.

Explicit gateway runs set `CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST=1` so settings files cannot replace the selected provider, and `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1` to remove provider credentials from Bash, hooks, and MCP subprocesses. Agent env overrides cannot disable these flags. Use a Claude version that supports both controls; validate shell-tool behavior on the target Linux image because credential scrubbing also uses PID namespace isolation. See the [Claude environment variable reference](https://code.claude.com/docs/en/env-vars).
