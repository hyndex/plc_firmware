from pathlib import Path

Import("env")

project_dir = Path(env["PROJECT_DIR"]).resolve()
evse_root = project_dir.parent

jpv2g_root = (evse_root / "jpv2g").resolve()
cbslac_root = (evse_root / "cbslac").resolve()
cbmodules_root = (evse_root / "jpmodules").resolve()
mcp_root = (cbmodules_root / "platformio" / "esp32s3_cbmodules_dual_mxr" / "lib" / "mcp2515" / "src").resolve()

env.Append(
    CPPPATH=[
        str(jpv2g_root / "include"),
        str(jpv2g_root / "3rd_party" / "cbv2g" / "include"),
        str(jpv2g_root / "3rd_party" / "cbv2g" / "lib"),
        str(cbslac_root / "include"),
        str(cbslac_root / "3rd_party"),
        str(cbmodules_root / "include"),
        str(mcp_root),
    ]
)

build_dir = Path(env.subst("$BUILD_DIR"))

env.BuildSources(
    str(build_dir / "jpv2g_src"),
    str(jpv2g_root / "src"),
)
env.BuildSources(
    str(build_dir / "cbv2g_src"),
    str(jpv2g_root / "3rd_party" / "cbv2g" / "lib" / "cbv2g"),
)
env.BuildSources(
    str(build_dir / "cbslac_src"),
    str(cbslac_root / "src"),
)
env.BuildSources(
    str(build_dir / "hash_src"),
    str(cbslac_root / "3rd_party" / "hash_library"),
)
env.BuildSources(
    str(build_dir / "cbmodules_src"),
    str(cbmodules_root / "src"),
)
env.BuildSources(
    str(build_dir / "mcp_src"),
    str(mcp_root),
)
