# Contributing

Thank you for your interest in contributing to DecentDb!

## Ways to Contribute

### Code Contributions

- Bug fixes
- New features
- Performance improvements
- Documentation

### Non-Code Contributions

- Bug reports
- Feature requests
- Documentation improvements
- Test cases
- Benchmarks
- Translations

## Getting Started

### Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/decentdb.git
cd decentdb
```

### Set Up Development Environment

```bash
# Install Nim (see Building guide)
# Install dependencies
nimble install

# Build the project
nimble build

# Run tests
nimble test
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/my-feature
# or
git checkout -b fix/bug-description
```

### 2. Make Changes

- Write code
- Add tests
- Update documentation

### 3. Test Your Changes

```bash
# Run all tests
nimble test

# Run specific test
nim c -r tests/nim/test_your_feature.nim

# Build with strict checks
nimble build
```

### 4. Commit

```bash
git add .
git commit -m "type: description"
```

Commit message format:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Tests
- `perf:` Performance
- `refactor:` Code refactoring

Examples:
```
feat: Add IN operator support
fix: Handle NULL in aggregate functions
docs: Update SQL reference for JOINs
test: Add crash test for torn writes
```

### 5. Push and Create Pull Request

```bash
git push origin feature/my-feature
```

Then open a PR on GitHub with:
- Clear description
- What changed and why
- Test results
- Related issues

## Code Guidelines

### Nim Style

Follow [NEP-1](https://nim-lang.org/docs/nep1.html):

```nim
# Good
proc calculateTotal(items: seq[Item]): int64 =
  result = 0
  for item in items:
    result += item.price

# Bad
proc calculate_total(items:seq[Item]):int64=
result=0
for i in items:result+=i.price
```

Key points:
- PascalCase for types
- camelCase for variables, procs
- 2 spaces indentation
- Max 80-100 chars per line

### Error Handling

Always check results:

```nim
let res = someOperation()
if not res.ok:
  return err[Type](res.err.code, res.err.message)
```

Never ignore errors in production code.

### Testing

Every change needs tests:

```nim
suite "My Feature":
  test "basic functionality":
    let res = myFeature()
    check res.ok
    check res.value == expected
  
  test "error handling":
    let res = myFeature(invalid_input)
    check not res.ok
    check res.err.code == ERR_SQL
```

### Documentation

Document public APIs:

```nim
## Calculate page utilization percentage
## 
## Returns 0.0-100.0 representing percentage of page space used
proc calculatePageUtilization*(tree: BTree, pageId: PageId): Result[float]
```

## Areas Needing Help

### High Priority

- [ ] Cost-based query optimizer
- [x] Online schema migrations (ALTER TABLE) - **IMPLEMENTED in v1.0.0**
- [ ] Additional SQL functions
- [ ] Performance benchmarks
- [ ] More crash test scenarios

### Medium Priority

- [ ] Additional VFS implementations
- [ ] Query caching improvements
- [ ] Compression support
- [ ] Better error messages
- [ ] GUI tools

### Documentation

- [ ] More code examples
- [ ] Tutorial videos
- [ ] Language bindings (Python, etc.)
- [ ] Performance tuning guides

## Submitting Issues

### Bug Reports

Include:
- DecentDb version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Error messages
- Minimal test case

Example:
```
**Version:** 0.0.1
**OS:** Ubuntu 22.04

**Steps:**
1. CREATE TABLE t (id INT)
2. INSERT INTO t VALUES (1)
3. SELECT * FROM t WHERE id = 2

**Expected:** Empty result
**Actual:** Returns row with id=1

**Error:** None
```

### Feature Requests

Include:
- Use case description
- Proposed solution
- Alternatives considered
- Additional context

## Pull Request Process

1. **Ensure tests pass**
   ```bash
   nimble test
   ```

2. **Update documentation**
   - Add to relevant .md files
   - Update CHANGELOG.md
   - Add code comments

3. **Follow commit conventions**
   - Clear, descriptive messages
   - Reference issues: "Fixes #123"

4. **Request review**
   - Tag maintainers
   - Respond to feedback
   - Make requested changes

5. **Wait for CI**
   - All checks must pass
   - Reviewer approval required

## Code Review

### What We Look For

- **Correctness:** Does it work? Are edge cases handled?
- **Tests:** Are there tests? Do they cover edge cases?
- **Style:** Does it follow Nim conventions?
- **Documentation:** Is it documented?
- **Performance:** Any obvious issues?

### Review Process

1. Automated checks (CI)
2. Maintainer review
3. Address feedback
4. Final approval
5. Merge

## Development Environment

### Recommended Setup

- VS Code with Nim extension
- Or Vim/Neovim with nimlsp
- Git with GPG signing
- GitHub CLI (optional)

### Useful Commands

```bash
# Format code
nimpretty src/*.nim

# Check for issues
nim check src/decentdb.nim

# Profile build
nim c -d:release --profiler:on src/decentdb.nim

# Memory check
valgrind --leak-check=full ./decentdb ...
```

## Community

### Communication

- GitHub Issues: Bug reports, features
- GitHub Discussions: Questions, ideas
- Pull Requests: Code contributions

### Code of Conduct

- Be respectful
- Welcome newcomers
- Focus on constructive feedback
- Assume good intent

## Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Credited in relevant documentation

## Questions?

- Check existing issues
- Read the documentation
- Open a discussion
- Ask in a PR comment

Thank you for contributing to DecentDb!
