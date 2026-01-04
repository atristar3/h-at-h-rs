# Contributing to H@H-rs

First off, thank you for considering contributing to H@H-rs! It's people like you that make H@H-rs such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by our commitment to creating a welcoming and inclusive environment. Please be respectful and constructive in all interactions.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When creating a bug report, please include:

- **A clear and descriptive title**
- **Steps to reproduce the behavior**
- **Expected behavior**
- **Actual behavior**
- **Your environment** (OS, Rust version, Docker version if applicable)
- **Relevant logs or error messages**

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

- **A clear and descriptive title**
- **Detailed description of the proposed functionality**
- **Why this enhancement would be useful**
- **Possible implementation approach** (if you have ideas)

### Pull Requests

1. **Fork the repo** and create your branch from `main`
2. **Make your changes** following our coding standards
3. **Add tests** if applicable
4. **Ensure the test suite passes** (`cargo test`)
5. **Run clippy** (`cargo clippy -- -D warnings`)
6. **Format your code** (`cargo fmt`)
7. **Update documentation** if needed
8. **Write a clear commit message**

## Development Setup

### Prerequisites

- Rust 1.75 or later
- SQLite (for local development)
- Docker (optional, for container testing)

### Building

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/h-at-h-rs.git
cd h-at-h-rs

# Build
cargo build

# Run tests
cargo test

# Run clippy
cargo clippy -- -D warnings

# Format code
cargo fmt
```

### Running Locally

```bash
# Copy example environment
cp .env.example .env
# Edit .env with your test credentials

# Run the application
cargo run
```

### Running Benchmarks

```bash
cargo bench
```

## Coding Standards

### Rust Style

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `rustfmt` for formatting (default settings)
- All public items must have documentation
- Avoid `unwrap()` in library code; use proper error handling
- Use `thiserror` for error types

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

### Documentation

- Document all public APIs
- Include examples in documentation where helpful
- Keep README.md up to date with new features
- Update CHANGELOG.md for notable changes

## Testing

### Unit Tests

Place unit tests in the same file as the code being tested:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        // ...
    }
}
```

### Integration Tests

Place integration tests in the `tests/` directory.

### Benchmarks

Place benchmarks in the `benches/` directory using Criterion.

## Release Process

Releases are automated via GitHub Actions when a tag is pushed:

```bash
git tag v0.2.0
git push origin v0.2.0
```

This will:
1. Create a GitHub release
2. Build binaries for all platforms
3. Publish Docker images to GHCR

## Questions?

Feel free to open an issue with the `question` label if you have any questions about contributing.

Thank you for contributing! 🚀
