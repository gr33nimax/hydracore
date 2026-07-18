# Etonify performance checks

`verify_etonify_performance.sh` is the repeatable core gate used by CI. It:

- opens and closes 64 real local XHTTP sessions;
- checks that the session registry is empty after every close;
- checks goroutine recovery on every platform and file descriptor recovery on Linux;
- runs the VLESS Encryption handshake benchmark three times;
- rejects a run that exceeds the versioned time, memory, or allocation ceilings in `ETONIFY_PERFORMANCE_BASELINE`.

Run it from the repository root:

```bash
bash release/verify_etonify_performance.sh
```

Android PSS, RSS, file descriptors, and process CPU depend on Flutter, ART, the device, and the active VPN workload, so a Go runner cannot measure them honestly. After installing a release APK, keep the VPN active and exercise URLTest, network switching, screen off/on, and connect/disconnect while running:

```powershell
powershell -ExecutionPolicy Bypass -File release/sample_android_resources.ps1
```

Use `-SampleCount 1` for a quick diagnostic snapshot without waiting for a soak interval.

The sampler writes CSV after every measurement and fails at the end if the release exceeds the Android ceilings in `ETONIFY_PERFORMANCE_BASELINE`. Only change those ceilings together with a recorded device result and an explanation in the core release notes.
