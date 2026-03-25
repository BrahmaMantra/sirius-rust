use std::env;
use std::path::PathBuf;

fn main() {
    let duckdb_dir = PathBuf::from("duckdb");
    let header_path = duckdb_dir.join("src/include/duckdb.h");

    // 如果 DuckDB submodule 还没初始化，跳过 bindgen（允许 cargo test 在 CI 用 bundled）
    if !header_path.exists() {
        println!(
            "cargo:warning=DuckDB submodule not found at {:?}, skipping bindgen",
            header_path
        );
        // 生成空的 bindings 文件，让编译通过
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("duckdb_bindings.rs");
        std::fs::write(&out_path, "// DuckDB bindings placeholder - submodule not initialized\n")
            .expect("Failed to write placeholder bindings");
        return;
    }

    let ext_header_path = duckdb_dir.join("src/include/duckdb_extension.h");

    println!("cargo:rerun-if-changed={}", header_path.display());
    println!("cargo:rerun-if-changed={}", ext_header_path.display());

    let include_dir = duckdb_dir.join("src/include");
    let include_arg = format!("-I{}", include_dir.display());

    // 1) 从 duckdb.h 生成主 FFI 绑定（类型 + 函数声明）
    let bindings = bindgen::Builder::default()
        .header(header_path.to_str().unwrap())
        .allowlist_function("duckdb_.*")
        .allowlist_type("duckdb_.*")
        .allowlist_var("DUCKDB_.*")
        .clang_arg(&include_arg)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Failed to generate DuckDB bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("duckdb_bindings.rs");
    bindings
        .write_to_file(&out_path)
        .expect("Failed to write bindings");

    // 2) 从 duckdb_extension.h 生成扩展 API 绑定（duckdb_ext_api_v1 + duckdb_extension_access）
    //    用 DUCKDB_API_EXCLUDE_FUNCTIONS 避免函数声明重复
    if ext_header_path.exists() {
        let ext_bindings = bindgen::Builder::default()
            .header(ext_header_path.to_str().unwrap())
            .clang_arg(&include_arg)
            .clang_arg("-DDUCKDB_API_EXCLUDE_FUNCTIONS")
            .allowlist_type("duckdb_ext_api_v1")
            .allowlist_type("duckdb_extension_access")
            .allowlist_var("DUCKDB_EXTENSION_API_VERSION_.*")
            // 用 raw_line 引入主 bindings 中已有的类型
            .raw_line("use super::sys::*;")
            // 不递归生成已有类型
            .blocklist_type("duckdb_connection")
            .blocklist_type("duckdb_database")
            .blocklist_type("duckdb_extension_info")
            .blocklist_type("duckdb_state")
            .blocklist_type("duckdb_type")
            .blocklist_type("duckdb_logical_type")
            .blocklist_type("duckdb_data_chunk")
            .blocklist_type("duckdb_vector")
            .blocklist_type("duckdb_function_info")
            .blocklist_type("duckdb_aggregate_function")
            .blocklist_type("duckdb_aggregate_function_set")
            .blocklist_type("duckdb_aggregate_state")
            .blocklist_type("duckdb_result")
            .blocklist_type("duckdb_config")
            .blocklist_type("duckdb_value")
            .blocklist_type("duckdb_table_function")
            .blocklist_type("duckdb_replacement_scan_info")
            .blocklist_type("duckdb_prepared_statement")
            .blocklist_type("duckdb_pending_result")
            .blocklist_type("duckdb_appender")
            .blocklist_type("duckdb_profiling_info")
            .blocklist_type("idx_t")
            .blocklist_type("DUCKDB_TYPE")
            .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
            .generate()
            .expect("Failed to generate DuckDB extension API bindings");

        let ext_out_path =
            PathBuf::from(env::var("OUT_DIR").unwrap()).join("duckdb_ext_bindings.rs");
        ext_bindings
            .write_to_file(&ext_out_path)
            .expect("Failed to write extension bindings");
    }

    // 链接 DuckDB 静态库（仅集成测试需要，loadable extension 不需要）
    // loadable extension 的 DuckDB 符号由宿主进程在运行时提供
    let link_duckdb = env::var("CARGO_FEATURE_LINK_DUCKDB").is_ok();
    let duckdb_lib_dir = duckdb_dir.join("build_release/src");
    if link_duckdb && duckdb_lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", duckdb_lib_dir.display());
        println!("cargo:rustc-link-lib=static=duckdb_static");

        // DuckDB 扩展加载器（dummy 提供 LoadAllExtensions 符号）
        let ext_dir = duckdb_dir.join("build_release/extension");
        if ext_dir.exists() {
            println!("cargo:rustc-link-search=native={}", ext_dir.display());
            println!("cargo:rustc-link-lib=static=dummy_static_extension_loader");
            println!("cargo:rustc-link-lib=static=duckdb_generated_extension_loader");
        }

        // DuckDB 的第三方依赖
        let third_party_dir = duckdb_dir.join("build_release/third_party");
        let third_party_libs = [
            ("fastpforlib", "duckdb_fastpforlib"),
            ("fmt", "duckdb_fmt"),
            ("fsst", "duckdb_fsst"),
            ("hyperloglog", "duckdb_hyperloglog"),
            ("libpg_query", "duckdb_pg_query"),
            ("mbedtls", "duckdb_mbedtls"),
            ("miniz", "duckdb_miniz"),
            ("re2", "duckdb_re2"),
            ("skiplist", "duckdb_skiplistlib"),
            ("utf8proc", "duckdb_utf8proc"),
            ("yyjson", "duckdb_yyjson"),
            ("zstd", "duckdb_zstd"),
        ];
        for (dir, lib) in &third_party_libs {
            let lib_dir = third_party_dir.join(dir);
            if lib_dir.exists() {
                println!("cargo:rustc-link-search=native={}", lib_dir.display());
                println!("cargo:rustc-link-lib=static={lib}");
            }
        }

        // 系统库
        println!("cargo:rustc-link-lib=dylib=stdc++");
        println!("cargo:rustc-link-lib=dylib=pthread");
        println!("cargo:rustc-link-lib=dylib=dl");
    }

    // Compile CUDA kernels only when gpu feature is enabled
    #[cfg(feature = "gpu")]
    {
        println!("cargo:rerun-if-changed=src/cuda/kernels/");

        cc::Build::new()
            .cuda(true)
            .flag("-gencode=arch=compute_70,code=sm_70") // Volta
            .flag("-gencode=arch=compute_75,code=sm_75") // Turing
            .flag("-gencode=arch=compute_80,code=sm_80") // Ampere
            .flag("-gencode=arch=compute_86,code=sm_86") // Ampere
            .file("src/cuda/kernels/sum.cu")
            .file("src/cuda/kernels/count.cu")
            .compile("sirius_kernels");
    }
}
