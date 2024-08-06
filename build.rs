fn main() -> anyhow::Result<()> {
    tonic_build::compile_protos("proto/emu.proto")?;
    Ok(())
}
