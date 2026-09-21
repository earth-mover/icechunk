{
  description = "Icechunk flake, python and rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    fenix,
    ...
  }: let
    inherit (nixpkgs) lib;

    # Systems we build dev shells for. mold and the manylinux LD_LIBRARY_PATH
    # are Linux-only and gated below; macOS uses the default Apple toolchain.
    systems = [
      "x86_64-linux"
      "aarch64-darwin"
    ];
    forAllSystems = lib.genAttrs systems;
  in {
    packages = forAllSystems (system: {
      default = fenix.packages.${system}.stable.toolchain;
    });

    # Development shell: Python via uv plus the Rust toolchain and tooling.
    devShells = forAllSystems (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};

        # ruff 0.16.4, matching `ruff==0.16.4` in icechunk-python/pyproject.toml.
        ruff = pkgs.ruff;

        # Use Python 3.12 from nixpkgs
        python = pkgs.python312;

        # Rust toolchain plus the wasm32-wasip1-threads std for `just wasm-build`.
        rustToolchain = fenix.packages.${system}.combine [
          fenix.packages.${system}.stable.toolchain
          fenix.packages.${system}.targets.wasm32-wasip1-threads.stable.rust-std
        ];

        # Official wasi-sdk sysroot; nixpkgs' wasilibc (single-threaded,
        # split outputs) can't serve as a --sysroot.
        wasiSysroot = pkgs.fetchzip {
          url = "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-27/wasi-sysroot-27.0.tar.gz";
          hash = "sha256-EAvyfHyT+mcyHilyqrjg3I1eih5dZyfXynDafTP4p3g=";
        };

        # Free-threaded CPython for the py314t wheel build.
        # link only python3.14t so the package's bin/python3 doesn't shadow python312.
        python314t = pkgs.runCommand "python314t-bin" {} ''
          mkdir -p $out/bin
          ln -s ${pkgs.python314FreeThreading}/bin/python3.14t $out/bin/python3.14t
        '';
      in rec {
        # Without this, a bare `nix develop` falls through to `packages.default`
        # (the plain fenix toolchain), which has no wasm32-wasip1-threads std
        # and none of the tooling, so `just wasm-build` fails there.
        default = impure;

        # Named `impure` because it manages virtualenvs with uv rather than
        # Nix; it also undoes the dependency leakage done by Nixpkgs Python
        # infrastructure.
        impure =
          pkgs.mkShell.override
          {
            # mold is an ELF-only linker; on macOS use the default Apple toolchain.
            stdenv =
              if pkgs.stdenv.hostPlatform.isLinux
              then pkgs.stdenvAdapters.useMoldLinker pkgs.clangStdenv
              else pkgs.stdenv;
          }
          {
            packages =
              [
                python
                pkgs.uv
                ruff

                rustToolchain
                pkgs.cargo-nextest # test runner
                pkgs.cargo-deny
                pkgs.cargo-edit
                pkgs.cargo-msrv
                pkgs.cargo-machete
                pkgs.cargo-llvm-cov

                pkgs.taplo # toml lsp server
                pkgs.awscli2
                pkgs.google-cloud-sdk
                pkgs.just # script launcher with a make flavor
                python.pkgs.semver # pysemver, for `just check-msrv`/`check-pixi-version`
                pkgs.alejandra # nix code formatter
                pkgs.markdownlint-cli2
                pkgs.flatbuffers
                pkgs.prek # pre-commit runner (`just py-pre-commit`)
                pkgs.cairo # mkdocs-material social plugin dlopens libcairo
                pkgs.nodejs_22 # icechunk-js
                pkgs.corepack_22 # provisions yarn per package.json packageManager
                python314t

                # necessary for reqwest
                pkgs.openssl
                pkgs.pkg-config
              ]
              ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [pkgs.mold];

            env =
              {
                # Prevent uv from managing Python downloads
                UV_PYTHON_DOWNLOADS = "never";

                # A version, not a path: a path request would also retarget
                # `uv pip install` away from the active venv (maturin develop).
                UV_PYTHON = "3.12";
                UV_PYTHON_PREFERENCE = "system";

                RUSTFLAGS = "-W unreachable-pub -W bare-trait-objects";

                # `just wasm-build`: absolute paths to unwrapped clang; wrapped
                # and Apple clang inject host flags that break the wasm32 build.
                WASI_SYSROOT = "${wasiSysroot}";
                CC_wasm32_wasip1_threads = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang";
                CXX_wasm32_wasip1_threads = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang++";
                AR_wasm32_wasip1_threads = "${pkgs.llvmPackages.llvm}/bin/llvm-ar";
              }
              // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
                # Python libraries often load native shared objects using dlopen(3).
                # Setting LD_LIBRARY_PATH makes the dynamic library loader aware of libraries without using RPATH for lookup.
                # libpython is needed by the pyo3 lib-test binary (no build.rs, so no rpath).
                LD_LIBRARY_PATH = lib.makeLibraryPath [
                  pkgs.stdenv.cc.cc
                  python
                  pkgs.zlib # manylinux wheels (numpy) expect a system libz.so.1
                  pkgs.cairo
                ];
              }
              // lib.optionalAttrs pkgs.stdenv.hostPlatform.isDarwin {
                # For dlopen'd libcairo and the rpath-less pyo3 lib-test's
                # libpython; tail keeps the system defaults this var replaces.
                DYLD_FALLBACK_LIBRARY_PATH = "${
                  lib.makeLibraryPath [
                    pkgs.cairo
                    python
                  ]
                }:/usr/local/lib:/usr/lib";
              };
            shellHook = ''
              unset PYTHONPATH
              # a leaked pixi env (e.g. via direnv) flips Justfile recipes
              # into their conda branches and taints library paths
              unset CONDA_PREFIX
            '';
          };
      }
    );
  };
}
