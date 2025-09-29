# Admin Commands

This document summarizes the admin-only controls available via DM.

Admin commands must be sent as direct messages to the bot from an authorized admin.

## Core Controls

- `/admin` — show quick help
- `/status` — show current admin-managed settings
- `/whatsoff` — list disabled features
- `/ai on|off` — enable/disable AI replies
- `/channels+dm on` — allow replies in both channels and DMs
- `/channels on` — allow replies in channels only
- `/dm on` — allow replies in DMs only
- `/autoping on|off` — toggle automatic queue delay pings
- `/<command> on|off` — enable or disable a specific command

## Command Aliases (Linking Commands)

Admins can define lightweight aliases so an unrecognized command points to an existing one.

- Syntax: `/newcmd = /existingcmd`
- Spacing around `=` is optional: `/new=/old`, `/new =/old`, `/new= /old`, etc.
- Either side may be the known command. Examples:
  - `/newspaper = /drudge` (maps `/newspaper` to `/drudge`)
  - `/drudge = /newspaper` (also maps `/newspaper` to `/drudge`)

On success, the alias is persisted to `commands_config.json` under `command_aliases` and becomes active immediately. Newly created aliases appear under “Other Commands” in the dashboard command list.

Notes:
- Reserved admin names (e.g., `/admin`, `/ai`, `/dm`) cannot be overridden.
- When both sides are known, the left-hand side becomes an alias of the right-hand side.

