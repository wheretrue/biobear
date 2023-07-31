build:
	cargo build
	maturin develop --release

test: build
	pytest

run-benchmarks:
	hyperfine --runs 2 \
		-n biopython 'python benchmarks/biopython-scan.py' \
		-n biobear 'python benchmarks/biobear-scan.py'
