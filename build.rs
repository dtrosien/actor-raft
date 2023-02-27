fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("entry", "#[derive(serde::Deserialize, serde::Serialize)]")
        .type_attribute(
            "AppendEntriesRequest",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .compile(&["proto/raft.proto"], &["proto"])?;
    Ok(())
}
