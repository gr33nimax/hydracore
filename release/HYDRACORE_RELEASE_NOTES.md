# HydraCore debug release notes

This prerelease ships the protocol-v9 `vk_parasite` transport: QUIC over four
required VK/TURN/DTLS paths, with four paths by default and up to twenty workers
in multiples of four.

Transport health is part of the typed runtime stream. Reports carry the outbound
tag and runtime generation; material state, challenge, lane, and failure changes
wake the existing stream without JSON polling across JNI.

The release contains separate Android client and Linux VPS runtimes. They must
come from the same release manifest and source commit. The VPS advertises
`call_vk_parasite_server`; the client advertises `call_vk_parasite_client`; both
advertise `call_vk_parasite_quic` and `call_vk_telemetry`.

CI verifies every release before publication. Assets are the Android AAR and
sources, three Android shared libraries, two Linux archives, and a signed bundle
manifest.
