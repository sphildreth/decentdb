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

## Workflows

### GitHub release binaries

- Workflow: `.github/workflows/release.yml`
- Trigger: tags `v0.1.*`, `v1.0.0-rc.*`, `v1.0.0`
- Output: release artifacts containing the DecentDb CLI and the native library for Linux/Windows/macOS.

### NuGet publishing

- Workflow: `.github/workflows/nuget.yml`
- Trigger: tags `v0.1.*`, `v1.0.0-rc.*`, `v1.0.0`
- Package: `Decent.MicroOrm`
- Target framework: `.NET 10` only (`net10.0`)

Required secret:

- `NUGET_API_KEY` (repo Actions secret)

## Creating a pre-release

1. Ensure `main` is green (tests).
2. Choose the next build number `X`.
3. Create and push the tag:

```bash
git tag -a v0.1.X -m "DecentDb 0.1.X (NuGet 1.0.0-rc.X)"
git push origin v0.1.X
```

GitHub Actions will:

- create a GitHub pre-release and attach binaries
- publish `Decent.MicroOrm` as `1.0.0-rc.X`
