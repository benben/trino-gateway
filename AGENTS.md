# Repository guidance

This is a public repository. Do not commit internal infrastructure details, private hostnames, cloud account identifiers, credentials, tokens, customer information, or diagnostic output containing those values. Use generic examples and runtime configuration. Keep local environment settings and raw test output outside the repository.

## Transaction-awareness development

- Preserve the standard Trino client protocol and existing behavior when the feature is disabled.
- Write reproducible regression tests before changing behavior. Record genuine baseline failures separately from infrastructure errors.
- Use multiple Gateway processes and a shared database when testing high-availability guarantees. Mock-only tests do not establish distributed correctness.
- Reject unknown transaction ownership. Never send an existing transaction to an arbitrary backend.
- Treat uncertain admissions, transactions, and result delivery as drain blockers. Do not equate cache expiry with successful completion.
- Review cutover races, retries, authentication boundaries, database failures, and backend restarts explicitly.
- Never claim recovery of an in-memory transaction after its coordinator is lost.
- Use isolated development namespaces with an explicit Kubernetes context. Never modify another application's resources during test setup.
- Keep fixtures, test commands, supported guarantees, and known limitations in this repository.

## Workflow

Use isolated worktrees for parallel changes. Do not discard changes made by another contributor. Review staged content for sensitive information before pushing. Keep commits focused and preserve a linear integration history.

Run the relevant tests and repository checks before declaring a change complete. A deliberately failing regression-test checkpoint must be clearly labeled; it is not a completed implementation.
