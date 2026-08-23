.DEFAULT_GOAL := help

.PHONY: help build test lint fmt

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z_-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build an optimized release binary
	cargo build --release --locked

test: ## Run the full test suite and doctests
	cargo test --locked --all-targets --all-features
	cargo test --locked --doc

lint: ## Check formatting and run Clippy
	cargo fmt --check
	cargo clippy --locked --all-targets --all-features -- -D warnings
	cargo check --locked --all-targets

fmt: ## Format the Rust sources
	cargo fmt
