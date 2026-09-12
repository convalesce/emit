# Releasing

One tag publishes everything. The versions in the tree must already agree
with it, or the workflow stops before uploading anything.

## Branches

`main` is where work lands, by pull request only. `release` is what gets
published: it only ever receives merges from `main`, and every tag points
at a commit on it. Three things enforce this, so a slip cannot publish:

- the workflow stops if the tagged commit is not on `release`;
- the `pypi*` and `maven-central` environments only deploy from `release`
  and `v*` tags, so a run from any other branch gets no credentials;
- rulesets block direct pushes, force pushes and deletion on both
  branches, and only administrators can create or move a `v*` tag.

## Cut a release

1. On `main`, set the version in all seven places, identically:
   `python/src/convalesce_emit/_version.py`, the four
   `python/plugins/*/src/*/_version.py`, `version` in `java/build.gradle`,
   and `VERSION` in `java/emit-core/.../emit/Version.java`. Each
   `pyproject.toml` reads its `_version.py`, so there is nothing else to
   edit. The workflow checks all seven against the tag, so a missed one
   stops the release rather than shipping a wrong `client_version`. Merge it
   with `ci` and `e2e` green.
2. Open a pull request from `main` into `release` and merge it. Both
   workflows run on `release` too.
3. Tag the release branch and push the tag:

   ```sh
   git fetch origin && git tag v0.1.1 origin/release && git push origin v0.1.1
   ```

The `publish` workflow then checks the tag against every declared version,
builds and uploads the five Python packages to PyPI, builds, signs and
deploys the two jars to Maven Central's staging area, and creates the
GitHub release with generated notes.

## Rehearse first

The workflow can be run by hand from the Actions tab with *Upload to
TestPyPI* ticked. That builds and uploads the same five packages to
https://test.pypi.org, from whatever ref is chosen, and touches nothing
else. It needs the `testpypi` environment registered as a trusted publisher
on TestPyPI the same way as below. Do this once before the first real tag,
because the first upload to PyPI claims the names.

Until Maven Central is configured, a tag still publishes to PyPI; the
Central job notices the missing secrets and stops instead of failing.

## One-time setup

### PyPI

Trusted publishing, so no API token exists to leak or rotate.

- One GitHub environment per package, because PyPI allows a single pending
  publisher per owner, repository, workflow and environment: `pypi` for
  `convalesce-emit`, then `pypi-airflow`, `pypi-dagster`, `pypi-prefect` and
  `pypi-gx`. The rehearsal uses the same names with `testpypi` in front.
- On PyPI, add a *pending publisher* for each of the five names: owner
  `convalesce`, repository `emit`, workflow `publish.yml`, and that package's
  environment. The first upload claims the name.

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

It does not bump versions, and it does not publish from `main`. Both are
deliberate: a release is a decision recorded in the tree, and the tag is
the record.
