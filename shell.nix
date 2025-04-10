let
  # Pinned nixpkgs, deterministic. Last updated to nixos-unstable as of: 2024-12-10
  pkgs = import (fetchTarball
    "https://github.com/NixOS/nixpkgs/archive/8c1f3147639f009f09d2bfffc64bcf8485bf3fd2.tar.gz")
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
