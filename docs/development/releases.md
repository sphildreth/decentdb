# Releases

This project uses Git tags to drive both:

- GitHub Releases (engine binaries)
- NuGet releases for the .NET bindings

## Tag scheme

### Pre-RTM engine releases

- Tag: `v0.1.X`
- GitHub: pre-release
- NuGet: `1.0.0-rc.X`

Examples:

- `v0.1.1` -> NuGet `1.0.0-rc.1`
- `v0.1.13` -> NuGet `1.0.0-rc.13`

### Release candidates (optional explicit tags)

- Tag: `v1.0.0-rc.X`
- GitHub: pre-release
- NuGet: `1.0.0-rc.X`

### RTM

- Tag: `v1.0.0`
- GitHub: full release
- NuGet: `1.0.0`

### Patch releases

- Tag: `v1.0.Z` (e.g. `v1.0.1`)
- GitHub: full release
- NuGet: `1.0.Z`

## Workflows

### GitHub release binaries

- Workflow: `.github/workflows/release.yml`
- Trigger: tags `v*`
- Output: release artifacts containing the DecentDB CLI and the native library for Linux/Windows/macOS.

### NuGet publishing

- Workflow: `.github/workflows/nuget.yml`
- Trigger: tags `v*`
- Package: `DecentDB.MicroOrm`
- Target framework: `.NET 10` only (`net10.0`)
- Publishing targets:
  - **GitHub Packages**: Automatically published using `GITHUB_TOKEN` (no configuration needed)
  - **NuGet.org**: Published if `NUGET_API_KEY` secret is configured

Required secrets:

- `NUGET_API_KEY` (optional) - API key for publishing to NuGet.org
  - Obtain from: https://www.nuget.org/account/apikeys
  - Create a new API key with "Push" permission for the `DecentDB.MicroOrm` package
  - Add the secret to repository settings: Settings → Secrets and variables → Actions → New repository secret
  - If not configured, packages will still be published to GitHub Packages

Both publish steps use `continue-on-error: true`, so if one target fails, the other will still be attempted.

## Creating a pre-release

1. Ensure `main` is green (tests).
2. Choose the next build number `X`.
3. Create and push the tag:

```bash
git tag -a v0.1.X -m "DecentDB 0.1.X (NuGet 1.0.0-rc.X)"
git push origin v0.1.X
```

GitHub Actions will:

- create a GitHub pre-release and attach binaries
- publish `DecentDB.MicroOrm` as `1.0.0-rc.X`
