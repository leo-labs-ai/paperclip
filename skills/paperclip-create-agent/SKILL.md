---
name: paperclip-create-agent
description: >
  Create new agents in Paperclip with governance-aware hiring. "Use when asked
  to hire or create an agent" — e.g. "hire a CTO", "create a designer agent",
  "we need a backend engineer on the team". Covers adapter discovery, config
  comparison, prompt/config drafting, and submitting the hire request.
---

# Paperclip Create Agent Skill

Use this skill when you are asked to hire/create an agent.

## Preconditions

You need either board access or agent permission `can_create_agents=true` in your company. If you do not have this permission, keep the current task assigned to yourself and identify the missing hiring authority. A CEO or manager title does not grant hiring permission. Do not reassign the task or create an escalation task to bypass the denial. Use the applicable approval flow when available; otherwise save a human-input interaction on the current task with `resolverPolicy: "human_only"` and `continuationPolicy: "wake_assignee"`, then leave it `in_review`. Use the complete human-input payload in the `paperclip` skill; when the requesting user owns the decision, address it with their actual `addresseeUserId`. A human answer does not itself grant permission: recheck authorization before any hire.

All API calls use `curl -sS` with `-H "Authorization: Bearer $PAPERCLIP_API_KEY"`. Endpoint inventory, hire payload shape, and response/approval semantics: `references/api-reference.md`.

## Workflow

### Choose the API transport

On **Paperclip Runner**, use the advertised Paperclip tools. Use `get_task_context`
and `list_agents` for identity and team context, then `search_api` to discover the
configuration, instruction-template, icon, and `agent-hires` endpoints used below.
Call the discovered operations with `call_api`; the server supplies company and
authentication context. Read the returned schemas before drafting the hire.

The shell examples below apply to adapters that receive `PAPERCLIP_API_URL` and
`PAPERCLIP_API_KEY`. Paperclip Runner does not supply those variables. Do not
search workspace files for a server address or credentials to recreate that path.
If `search_api` / `call_api` are unavailable, report that hiring requires the
operator to enable runner API tools for this company. Preserve the proposed hire
in the conversation; do not claim an agent was created or substitute a temporary
subagent for the requested permanent hire.

### 1. Confirm identity and company context

`GET /api/agents/me`

### 2. Discover adapter configuration for this Paperclip instance

`GET /llms/agent-configuration.txt`, then the specific adapter you plan to use, e.g. `GET /llms/agent-configuration/claude_local.txt`.

### 3. Compare existing agent configurations

`GET /api/companies/$PAPERCLIP_COMPANY_ID/agent-configurations` — note naming, icon, reporting-line, and adapter conventions the company already follows.

### 4. Describe the role

Use a short role paragraph for a new agent: its identity and the responsibility
it owns. The [role examples](references/agent-instruction-templates.md) are
optional starting points; for other roles, use the
[baseline role guide](references/baseline-role-guide.md).

Company-specific instructions supplied by the requester take precedence. Do not
expand a role description into a generic operating manual. The harness supplies
Paperclip coordination, skill discovery, and task lifecycle guidance; repository
instructions and installed skills carry applicable work procedures. Avoid
adding heartbeat pointers, execution contracts, mandatory per-touch comments,
fixed reviewer routes, or catalogs of domain concepts to the hire's instructions.

Keep reporting lines in `reportsTo`, capabilities in `capabilities`, and skills
in `desiredSkills`. Add instruction detail only for a concrete company or role
requirement that those fields, the task, repository instructions, or installed
skills do not already express.

Template index and when-to-use guidance: `references/agent-instruction-templates.md`. Generic fallback: `references/baseline-role-guide.md`. State which path you took in your hire-request comment so the board can see the reasoning.

### 5. Discover allowed agent icons

`GET /llms/agent-icons.txt`

### 6. Draft the new hire config

- role / title / name; icon (required in practice); reporting line (`reportsTo`); adapter type
- `desiredSkills` from the company skill library when this role needs installed skills on day one
- if any `desiredSkills` or adapter settings expand browser access, external-system reach, filesystem scope, or secret-handling capability, justify each one in the hire comment
- adapter and runtime config aligned to this environment
- leave timer heartbeats off by default; only set `runtimeConfig.heartbeat.enabled=true` with an `intervalSec` when the role genuinely needs scheduled recurring work or the user explicitly asked for it
- if the role may handle private advisories or sensitive disclosures, confirm a confidential workflow exists first (dedicated skill or documented manual process)
- capabilities
- when supplying role instructions from step 4, send them as top-level `instructionsBundle.files["AGENTS.md"]` for managed-bundle adapters. Otherwise use the server default. Do not set `adapterConfig.promptTemplate` or `bootstrapPromptTemplate` for new agents.
- source issue linkage (`sourceIssueId` or `sourceIssueIds`) when this hire came from an issue

### 7. Review the draft against the quality checklist

Walk `references/draft-review-checklist.md` end-to-end and fix any item that does not pass.

### 8. Submit hire request

`POST /api/companies/$PAPERCLIP_COMPANY_ID/agent-hires`. Required fields: `name`, `role`, `title`, `icon`, `adapterType`, `adapterConfig` (with `cwd` + `model`); include `reportsTo`, `capabilities`, `instructionsBundle`, `runtimeConfig`, and source-issue linkage as drafted. Full payload shape + example: `references/api-reference.md`.

### 9. Handle governance state

- if the response has `approval`, the hire is `pending_approval`
- monitor and discuss on the approval thread (`GET/POST /api/approvals/<id>/comments`); link the approval to its source issue via `POST /api/issues/<issue-id>/approvals` if needed
- when the board approves, you will be woken with `PAPERCLIP_APPROVAL_ID`; fetch the approval and its linked issues (`GET /api/approvals/$PAPERCLIP_APPROVAL_ID` + `/issues`), then for each linked issue either close it if the approval resolved the request, or comment in markdown with links to the approval and next actions.

## References

- Template index and how to apply a template: `references/agent-instruction-templates.md`
- Individual role templates: `references/agents/`
- Generic baseline role guide (no-template fallback): `references/baseline-role-guide.md`
- Pre-submit draft-review checklist: `references/draft-review-checklist.md`
- Endpoint inventory, payload shapes, approval lifecycle: `references/api-reference.md`

## Harness and runner selection

Choose the underlying harness in `adapterType`, such as `codex_local` or
`claude_local`. New Codex agents use Paperclip Runner automatically on supported
execution targets. New Claude Code agents use Paperclip Runner on Apple Silicon,
Intel Mac, Linux x64 and Linux ARM64 with supported glibc. Unknown or unsupported
targets retain Legacy. Darwin SSH also defaults to Legacy; native execution
requires an explicit runner choice and matching remote assets. The native Claude profile is
`provider: acpx`, `acpxAgent: claude`, with ordinary model `claude-opus-5-5`.
Explicit models and existing saved runners remain intact. The saved response
may therefore have `adapterType: paperclip_runner` and a resolved provider;
use that returned configuration for later reads and edits.

The optional creation field `runner` accepts `auto`, `paperclip`, or `legacy`.
Use `legacy` when the user needs CLI settings the native runtime cannot honor.
For Claude this includes Chrome, custom commands/state paths, warm-process
controls, confinement and turn limits. Native Claude uses existing API-key and
subscription connections. Its configured effort requires provider acknowledgment. Setup and authentication errors never silently change runners.
Existing agents keep their saved execution after unrelated edits. An approved
hire uses the configuration reviewed by the board. `inheritRuntimeFrom: caller`
keeps its existing runtime inheritance and credential restrictions.

CLI equivalents: `paperclipai agent create --runner legacy --payload-json ...`
and `paperclipai agent hire --runner legacy --payload-json ...`.
