# Admin panel

The admin panel is a Vite/React SPA for `manager_api/http`. Its generated
`dist/` directory is committed and embedded in the Go binary when
`with_admin_panel` is enabled.

## Build tag

```bash
go build -tags with_admin_panel ./cmd/sing-box
```

Without the tag, the service is a stub and refuses to start.

## Update the embedded panel

When files under `web/` change, run:

```bash
make build_admin_panel
```

The target installs the web dependencies, builds the SPA, and runs
`cmd/internal/admin_panel_pack`. Commit the resulting `dist/` changes with the
source changes.

## Configuration

```json
{
  "services": [{
    "type": "admin-panel",
    "tag": "admin",
    "listen": "127.0.0.1",
    "listen_port": 8081
  }]
}
```

The browser stores the configured manager API URL and bearer key in local
storage. Bind the panel to a trusted interface and protect access accordingly.
