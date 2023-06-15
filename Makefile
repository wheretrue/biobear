build:
	cargo build
	maturin develop --release

test: build
	pytest
