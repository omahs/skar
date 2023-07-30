install:
	RUSTFLAGS="-C target-cpu=native" cargo install --path skar --profile maxperf
run:
	RUST_LOG="debug,reqwest=off,hyper=off,h2=off,datafusion=off,sqlparser=off" cargo run --release
