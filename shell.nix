#   let
#     # Pinned nixpkgs, deterministic. Last updated to nixpkgs-unstable as of: 2024-07-23
#     pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/68c9ed8bbed9dfce253cc91560bf9043297ef2fe.tar.gz") {};
#   
#     # Rolling updates, not deterministic.
#     # pkgs = import (fetchTarball("channel:nixpkgs-unstable")) {};
#   
#     alejandra =
#       (import (builtins.fetchTarball {
#         url = "https://github.com/kamadorueda/alejandra/tarball/3.0.0";
#         sha256 = "sha256:18jm0d5xrxk38hw5sa470zgfz9xzdcyaskjhgjwhnmzd5fgacny4";
#       }) {})
#       .outPath;
#   in
#     pkgs.mkShell.override {
#       stdenv = pkgs.stdenvAdapters.useMoldLinker pkgs.clangStdenv;
#     } {
#       packages = with pkgs; [
#         rustc
#         cargo
#         cargo-watch
#         cargo-nextest # test runner
#         cargo-deny
#         rust-analyzer # rust lsp server
#         rustfmt
#         clippy
#         taplo # toml lsp server
#   
#         awscli2
#         just # script launcher with a make flavor
#         alejandra # nix code formatter
#       ];
#   
#       shellHook = ''
#         export PYTHONPATH=".:$PYTHONPATH"
#   
#         export AWS_ACCESS_KEY_ID=minio123
#         export AWS_SECRET_ACCESS_KEY=minio123
#         export AWS_DEFAULT_REGION=us-east-1
#         export RUSTFLAGS="-W unreachable-pub -W bare-trait-objects"
#       '';
#     }
{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/68c9ed8bbed9dfce253cc91560bf9043297ef2fe.tar.gz") {} }:
  let
    overrides = (builtins.fromTOML (builtins.readFile ./rust-toolchain.toml));
    libPath = with pkgs; lib.makeLibraryPath [
      # load external libraries that you need in your rust project here
    ];
in
  pkgs.mkShell rec {
    buildInputs = with pkgs; [
      clang
      # Replace llvmPackages with llvmPackages_X, where X is the latest LLVM version (at the time of writing, 16)
      llvmPackages_17.bintools
      rustup
    ];
    RUSTC_VERSION = overrides.toolchain.channel;
    # https://github.com/rust-lang/rust-bindgen#environment-variables
    LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages_latest.libclang.lib ];
    shellHook = ''
      export PATH=$PATH:''${CARGO_HOME:-~/.cargo}/bin
      export PATH=$PATH:''${RUSTUP_HOME:-~/.rustup}/toolchains/$RUSTC_VERSION-x86_64-unknown-linux-gnu/bin/
      '';
    # Add precompiled library to rustc search path
    RUSTFLAGS = (builtins.map (a: ''-L ${a}/lib'') [
      # add libraries here (e.g. pkgs.libvmi)
    ]);
    LD_LIBRARY_PATH = libPath;
    # Add glibc, clang, glib, and other headers to bindgen search path
    BINDGEN_EXTRA_CLANG_ARGS =
    # Includes normal include path
    (builtins.map (a: ''-I"${a}/include"'') [
      # add dev libraries here (e.g. pkgs.libvmi.dev)
      pkgs.glibc.dev
    ])
    # Includes with special directory paths
    ++ [
      ''-I"${pkgs.llvmPackages_latest.libclang.lib}/lib/clang/${pkgs.llvmPackages_latest.libclang.version}/include"''
      ''-I"${pkgs.glib.dev}/include/glib-2.0"''
      ''-I${pkgs.glib.out}/lib/glib-2.0/include/''
    ];
  }
