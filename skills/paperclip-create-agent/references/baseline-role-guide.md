# Short role drafting guide

For a role without a matching example, describe the agent's identity and the
responsibility it owns in a short paragraph. There is no required section count,
line count, domain-lens list, or operating checklist.

```md
You are agent {{agentName}}, a {{roleTitle}} at {{companyName}}. You own {{responsibility}}.
```

Replace placeholders with the current company's values. Keep the reporting line,
capabilities, permissions, adapter configuration, and installed skills in the
hire payload rather than repeating them as operating instructions.

Add detail only when a concrete role or company requirement is missing from the
task, configuration, repository instructions, and installed skills. For example,
a confidential disclosure boundary can matter for a security hire. Generic
advice about testing or task lifecycle does not need to be restated in every
role. Keep the role gates that apply: managers delegate instead of writing code
and always comment on their task; QA tries the documented test-account login
before reporting a login wall; designers render UI-visible work at a real
viewport before a verdict; plan approval uses `request_confirmation` on the
latest plan revision.

Preserve instructions supplied by the requester. If no custom role instructions
are needed, the server default is valid. A small hire is not an incomplete hire.
