let
  # Pinned nixpkgs, deterministic. Last updated to nixos-unstable as of: 2024-10-06
  pkgs = import (fetchTarball
    "https://github.com/NixOS/nixpkgs/archive/7d49afd36b5590f023ec56809c02e05d8164fbc4.tar.gz")
    { };

  # Rolling updates, not deterministic.
  # pkgs = import (fetchTarball("channel:nixpkgs-unstable")) {};

  alejandra = (import (builtins.fetchTarball {
    url = "https://github.com/kamadorueda/alejandra/tarball/3.0.0";
    sha256 = "sha256:18jm0d5xrxk38hw5sa470zgfz9xzdcyaskjhgjwhnmzd5fgacny4";
  }) { }).outPath;
in pkgs.mkShell.override {
  stdenv = pkgs.stdenvAdapters.useMoldLinker pkgs.clangStdenv;
} {
  packages = with pkgs; [
    rustc
    cargo
    cargo-watch
    cargo-nextest # test runner
    cargo-deny
    rust-analyzer # rust lsp server
    rustfmt
    clippy
    taplo # toml lsp server

    awscli2
    just # script launcher with a make flavor
    alejandra # nix code formatter
    markdownlint-cli2
  ];

  shellHook = ''
    export PYTHONPATH=".:$PYTHONPATH"

    export AWS_ACCESS_KEY_ID=minio123
    export AWS_SECRET_ACCESS_KEY=minio123
    export AWS_DEFAULT_REGION=us-east-1
    export RUSTFLAGS="-W unreachable-pub -W bare-trait-objects"
  '';
}
