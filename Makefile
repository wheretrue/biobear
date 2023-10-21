build:
	cargo build --release
	maturin develop --release

test:
	cargo build
	maturin develop
	pytest

run-benchmarks:
	hyperfine --runs 2 \
		-n biopython 'python benchmarks/biopython-scan.py' \
		-n biobear 'python benchmarks/biobear-scan.py'
