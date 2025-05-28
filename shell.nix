let
  # Pinned nixpkgs, deterministic. Last updated to nixpkgs-unstable as of: 2025-05-22
  pkgs = import (fetchTarball
    "https://github.com/NixOS/nixpkgs/archive/fe51d34885f7b5e3e7b59572796e1bcb427eccb1.tar.gz")
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
    cargo-nextest # test runner
    cargo-deny
    rust-analyzer # rust lsp server
    rustfmt
    clippy
    taplo # toml lsp server

    awscli2
    google-cloud-sdk
    just # script launcher with a make flavor
    alejandra # nix code formatter
    markdownlint-cli2
    python3
  ];

  shellHook = ''
    export PYTHONPATH=".:$PYTHONPATH"
    export RUSTFLAGS="-W unreachable-pub -W bare-trait-objects"
  '';
}
