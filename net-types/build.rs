fn main() {
    capnpc::CompilerCommand::new()
        .file("skar_net_types.capnp")
        .run()
        .expect("compiling schema");
}
