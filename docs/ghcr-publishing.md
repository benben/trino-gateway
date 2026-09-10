# Personal-fork container publishing

The `Publish GHCR image` workflow builds this fork's `main` branch and publishes
`ghcr.io/benben/trino-gateway`. It also supports manual runs from `main`.
It does not publish to Docker Hub or another organization's registry.

The workflow uses Java 25 and the existing `docker/build.sh` image builder and
container smoke tests for `linux/amd64` and `linux/arm64`. The normal CI and
transaction-test workflows continue to run separately. The publishing workflow
does not replace those checks.

Images include OCI source and revision metadata. Each run publishes a
`sha-<full-commit>` tag and a run-specific staging tag. `main` and `latest` advance
only if the built commit is still the current branch head. The publication
summary gives the resulting multi-architecture digest.

Commit tags identify source revisions, but GHCR does not enforce their
immutability here. A rerun can produce different bytes because base images and
package dependencies can change. Pin deployments to `@sha256:<digest>`, not a
mutable tag, when an exact artifact is required.

## First publication and public visibility

Publishing uses the workflow's `GITHUB_TOKEN` with `packages: write`; no personal
access token is required. New GHCR packages are private by default, even when
their source repository is public. After the first successful publication, the
package owner must change `trino-gateway` to **Public** in the package settings.
The workflow does not change package visibility. See GitHub's
[Container registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

Verify the resulting digest with an unauthenticated registry client before
relying on anonymous pulls. A successful authenticated workflow push does not
prove that the package is public. If the package already exists without a
repository link, grant this repository Actions access before publishing.
