# Releasing

One tag publishes everything. The versions in the tree must already agree
with it, or the workflow stops before uploading anything.

## Cut a release

1. Set the version in all six places, identically:
   `python/src/convalesce_emit/_version.py`, the four
   `python/plugins/*/src/*/_version.py`, and `version` in
   `java/build.gradle`. Each `pyproject.toml` reads its `_version.py`, so
   there is nothing else to edit.
2. Merge to `main` with `ci` and `e2e` green.
3. Tag and push:

   ```sh
   git tag v0.1.0 && git push origin v0.1.0
   ```

The `publish` workflow then checks the tag against every declared version,
builds and uploads the five Python packages to PyPI, builds, signs and
deploys the two jars to Maven Central's staging area, and creates the
GitHub release with generated notes.

## One-time setup

### PyPI

Trusted publishing, so no API token exists to leak or rotate.

- Create the `pypi` environment in the repository settings.
- On PyPI, add a *pending publisher* for each of the five names
  (`convalesce-emit`, `convalesce-emit-airflow`, `-dagster`, `-prefect`,
  `-gx`): owner `convalesce`, repository `emit`, workflow `publish.yml`,
  environment `pypi`. The first upload claims the name.

### Maven Central

- Register the `io.convalesce` namespace on the Central portal. It is
  verified by a DNS TXT record on `convalesce.io`.
- Generate a portal *user token* and store it as `CENTRAL_USERNAME` and
  `CENTRAL_PASSWORD` on a `maven-central` environment.
- Generate a signing key, publish its public half to a key server, and
  store the armoured private key as `SIGNING_KEY` with its passphrase as
  `SIGNING_PASSWORD` on the same environment:

  ```sh
  gpg --armor --export-secret-keys <key-id> | pbcopy
  ```

- A deployment lands in staging. Release it from the portal's
  *Deployments* page. Central validates the POM, sources, javadoc and
  signatures on the way in and rejects a bundle that lacks any of them.

## What the workflow does not do

It does not bump versions, and it does not publish from a branch. Both are
deliberate: a release is a decision recorded in the tree, and the tag is
the record.
