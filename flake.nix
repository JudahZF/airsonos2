{
  description = "AirSonos2 Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    # cargo-deny in nixos-25.05 (0.18.2) cannot parse CVSS 4.0 advisories;
    # pull a newer one from nixos-25.11 (pinned commit).
    nixpkgs-cargo-deny.url = "github:NixOS/nixpkgs/b6018f87da91d19d0ab4cf979885689b469cdd41";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-cargo-deny,
      rust-overlay,
    }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            }
          )
        );
    in
    {
      devShells = forAllSystems (
        pkgs:
        let
          rustToolchain = pkgs.rust-bin.stable."1.88.0".default.override {
            extensions = [
              "clippy"
              "rustfmt"
              "rust-src"
            ];
            targets = [
              "x86_64-unknown-linux-gnu"
            ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = [
              rustToolchain
              nixpkgs-cargo-deny.legacyPackages.${pkgs.stdenv.hostPlatform.system}.cargo-deny
              pkgs.cargo-nextest
              pkgs.cargo-zigbuild
              pkgs.ffmpeg
              pkgs.pkg-config
              pkgs.openssl
              pkgs.rust-analyzer
              pkgs.zig
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
              pkgs.docker
            ];

            RUST_BACKTRACE = "1";
          };
        }
      );

      formatter = forAllSystems (pkgs: pkgs.nixfmt-rfc-style);
    };
}
