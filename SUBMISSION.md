# OpenAI submission notes

Use the remote MCP submission path with this production endpoint:

```text
https://mcp.inergroup.joveo.com/mcp
```

## Tool annotation justifications

After scanning the production MCP server, use these justifications for
`search_inergroup_job_listings` in the submission portal:

- `readOnlyHint: true` — The tool only searches and returns data from a
  server-managed job-listing snapshot. A tool call cannot create, update, or
  delete records, submit an application, send a form, or otherwise change
  external state.
- `openWorldHint: false` — Each tool call is limited to the bounded Inergroup
  job catalogue already stored in the server's validated local snapshot. It
  does not browse the public web, contact arbitrary external entities, or
  trigger a feed refresh.
- `destructiveHint: false` — The tool has no write or deletion capability and
  cannot perform irreversible actions. It only returns job information and
  validated external application links for the user to open themselves.
