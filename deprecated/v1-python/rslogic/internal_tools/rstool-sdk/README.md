# RealityScan SDK for Python

Python client for the RealityScan HTTP API. This package provides a `RealityScanClient` with resource groups for `project` and `node` endpoints plus a set of convenience wrappers for common commands.

## Features

- Typed models for project, node, and task responses
- Session lifecycle helpers (`create`, `open`, `close`)
- High-level command wrappers (e.g., `add_image`, `start`, `save`)
- Simple, testable httpx-based transport

## Requirements

- Python 3.13+

## Installation

If you are using a local checkout:

```
pip install -e .
```

## Quick start

```python
from realityscan_sdk import RealityScanClient

client = RealityScanClient(
	base_url="http://localhost:8000",
	auth_token="<your-auth-token>",
	client_id="<your-client-id>",
	app_token="<your-app-token>",
)

# Start or open a session
client.project.create()

# Run commands
client.project.add_image("/path/to/image.jpg")
client.project.start()

# Check project status
status = client.project.status()
print(status)

client.project.close()
client.close()
```

## Usage notes

- Most `project` endpoints require a session header. Call `client.project.create()` or `client.project.open()` first.
- `RealityScanClient` updates `client.session` automatically from response headers.
- Use `with RealityScanClient(...) as client:` to ensure the HTTP client is closed.

## File handling quirks (not included in this SDK)

This SDK focuses strictly on the HTTP API. Any file-handling automation (SMB/NAS access, staging, bulk copies, etc.) is intentionally **not** included and should live in your own project.

RealityScan node server session storage is controlled by the `dataRoot` parameter. Ensure your environment sets this value appropriately, for example:

- `name`: `dataRoot`
- `description`: Root path for session storage directory
- `defaultValue`: `%LOCALAPPDATA%\Epic Games\RealityScan\RSNodeData`
- `type`: `string`

If your workflow requires shared storage, consider using something like SMB tooling (for example, `smbclient`) to:

- Authenticate to a network share
- Use UNC-style paths on Windows or equivalent share paths
- Stage imagery under a session-specific folder layout (e.g., `sessions/<session>/_data/Imagery`)
- Filter and copy files in parallel as needed

Keep credentials and environment-specific paths outside the SDK and adapt them to your network and storage conventions.

## Common tasks

### Open an existing project

```python
client.project.open(guid="<project-guid>")
```

### List projects from the node

```python
projects = client.node.projects()
```

### Run a command group

```python
task = client.project.command_group({
	"commandCall": [
		{"name": "newScene"},
		{"name": "save", "param1": "/path/to/project.rsproj"},
	]
})
print(task.taskID)
```

## Testing

```
pytest
```

