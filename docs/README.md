# Documentation

This directory contains the DecentDB documentation site built with [MkDocs](https://www.mkdocs.org/) and [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Local Development

### Prerequisites

```bash
pip install mkdocs-material
pip install mkdocs-minify-plugin
```

### Serve Locally

```bash
mkdocs serve
```

The site will be available at http://127.0.0.1:8000

### Build

```bash
mkdocs build
```

Output goes to `site/` directory.

## Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch.

The live site is at: https://decentdb.org

## Custom Domain Setup

To use decentdb.org as the custom domain:

1. Add a CNAME file to the `docs/` directory:
   ```
   echo "decentdb.org" > docs/CNAME
   ```

2. Configure DNS with your domain provider:
   - A Record: `@` pointing to GitHub Pages IPs:
     - 185.199.108.153
     - 185.199.109.153
     - 185.199.110.153
     - 185.199.111.153
   - Or CNAME Record: `www` pointing to `sphildreth.github.io`

3. Enable custom domain in GitHub repository settings

## Structure

- `index.md` - Home page
- `getting-started/` - Installation and quick start guides
- `user-guide/` - SQL reference, data types, performance tuning
- `api/` - API documentation (Nim API, CLI reference)
- `architecture/` - Technical documentation
- `development/` - Building, testing, contributing
- `design/` - Design documents (PRD, SPEC, ADRs)
- `about/` - License, changelog

## Adding Content

1. Create new `.md` files in appropriate directories
2. Update `mkdocs.yml` nav section to add to table of contents
3. Use Material for MkDocs features like admonitions, tabs, etc.

## Style Guide

- Use sentence case for headings
- Include code examples for all features
- Cross-reference related documentation
- Keep line length under 100 characters for readability
