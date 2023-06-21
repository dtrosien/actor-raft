fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // needed to be buildable on linux
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

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // needed to be buildable on linux
        .compile(&["proto/raft_client.proto"], &["proto"])?;

    Ok(())
}
