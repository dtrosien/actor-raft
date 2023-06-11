fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("entry", "#[derive(serde::Deserialize, serde::Serialize)]")
        .type_attribute(
            "AppendEntriesRequest",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "SessionInfo",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .compile(&["proto/raft_server.proto"], &["proto"])?;

    tonic_build::configure().compile(&["proto/raft_client.proto"], &["proto"])?;

    Ok(())
}
