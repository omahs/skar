install:
	RUSTFLAGS="-C target-cpu=native" cargo install --path skar --profile maxperf
install_nightly:
	RUSTFLAGS="-C target-cpu=native" cargo +nightly install --path skar --profile maxperf --features arrow_simd
run:
	RUST_LOG="debug,reqwest=off,hyper=off,h2=off,datafusion=off,sqlparser=off" cargo run --release
