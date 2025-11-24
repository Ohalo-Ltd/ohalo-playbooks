# Contributing to Graph RAG

## Development Workflow

1. Create a feature branch from `main`
2. Make your changes
3. Run tests and linting
4. Submit a pull request

## Branch Naming

- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Test additions/updates

## Commit Messages

Use clear, present-tense commit messages:

```
Add vector search tool
Fix Neo4j connection handling
Update schema discovery documentation
Refactor ingestion pipeline
```

## Code Style

### Python (Backend)

- Use Black for formatting (line length 88)
- Use Ruff for linting
- Use mypy for type checking
- Write docstrings for all public functions

```bash
cd backend
uv run black .
uv run ruff check --fix .
uv run mypy .
```

### TypeScript (Frontend)

- Use ESLint with Next.js config
- Use Prettier for formatting
- Follow React best practices
- Use TypeScript strict mode

```bash
cd frontend
npm run lint
```

## Testing

### Backend Tests

```bash
cd backend
uv run pytest              # Run all tests
uv run pytest -m unit      # Unit tests only
uv run pytest -m integration  # Integration tests
uv run pytest --cov        # With coverage
```

### Frontend Tests

```bash
cd frontend
npm test
```

## Pull Request Checklist

- [ ] All tests pass
- [ ] Code is formatted and linted
- [ ] Type checks pass
- [ ] Documentation is updated
- [ ] No secrets or credentials in code
- [ ] PR description explains the changes

## Code Review

All pull requests require at least one approval before merging.

Reviewers should check:
- Code quality and style
- Test coverage
- Performance implications
- Security considerations
- Documentation completeness

## Getting Help

- Open an issue for bugs or feature requests
- Ask questions in pull request discussions
- Check existing documentation in `docs/`
