# Security

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use the *Report a
vulnerability* form under this repository's Security tab, which opens a
private advisory that only maintainers can read. You will get an
acknowledgement within a few days and a fix or a decision as soon as one is
possible.

## Supported versions

The latest 0.x release of each package receives fixes. Older releases do
not.

## What these libraries do with data

Each plugin forwards a tool's own callback payload to the configured
endpoint over HTTPS, with the ingest key in the Authorization header. Two
properties are deliberate and worth reporting if you find them broken:

- **Row values never cross.** The Great Expectations action drops sample
  values and forwards counts. A payload from any plugin carrying customer
  data rows is a bug.
- **The key never appears anywhere but the header.** It is not logged, not
  placed in the envelope, and not printed by `CONVALESCE_DRY_RUN`.
