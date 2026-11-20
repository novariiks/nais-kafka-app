## Git & Commit Conventions

### Agent Boundaries
- **No auto-commits:** Keep changes in the working tree unless explicitly told to commit.
- **No branch switching:** Do not create, switch, or delete branches without permission.

### Commit Format
Adhere to [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/):
```text
<type>[optional scope]: <description>
[optional body]
[optional footer(s)]
```

### Types
- `feat`: New feature (SemVer `MINOR`)
- `fix`: Bug fix (SemVer `PATCH`)
- `docs`: Documentation updates
- `style`: Formatting/linting (no logic changes)
- `refactor`: Code restructuring
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Build system or dependency updates
- `ci`: CI/CD changes
- `chore`: Maintenance tasks
- `revert`: Reverting prior commits

### Rules
- **Tone & Casing:** Imperative, present tense (e.g., `add auth`), lowercase, no trailing period.
- **Length:** Subject line under 72 characters.
- **Scopes:** Lowercase module name in parentheses (e.g., `feat(api):`).
- **Breaking Changes:** Append `!` to the type/scope and include a `BREAKING CHANGE:` footer (SemVer `MAJOR`).
- **Atomicity:** One logical change per commit.