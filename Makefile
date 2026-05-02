all: basic_check

basic_check: lint test doc

lint:
	cargo fmt
	cargo clippy --no-deps --all-targets -- -D warnings

fmt:
	cargo fmt

test:
	cargo test

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --document-private-items --no-deps

check:
	RUSTFLAGS="-D warnings" cargo check --all-targets

clean:
	cargo clean

.PHONY: all basic_check lint fmt test doc check clean
