{
  description = "RsLogic v2 Rust client orchestration and RealityScan pipeline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  };

  outputs =
    { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
      supportedSystems = [ "x86_64-linux" ];
      forAllSystems = lib.genAttrs supportedSystems;
      workspaceSource = lib.fileset.unions [
        ./Cargo.lock
        ./Cargo.toml
        ./crates
      ];
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          src = lib.fileset.toSource {
            root = ./.;
            fileset = workspaceSource;
          };
        in
        rec {
          rslogic-client = pkgs.rustPlatform.buildRustPackage {
            pname = "rslogic-client";
            version = "0.1.0-${self.shortRev or "dirty"}";

            inherit src;
            cargoLock.lockFile = ./Cargo.lock;

            cargoBuildFlags = [
              "-p"
              "rslogic-agent"
              "-p"
              "rslogic-worker"
            ];

            cargoTestFlags = [
              "-p"
              "rslogic-protocol"
              "-p"
              "rslogic-agent"
              "-p"
              "rslogic-worker"
              "-p"
              "rslogic-realityscan"
            ];

            postInstall = ''
              install -Dm755 "$(find target -path '*/release/rslogic-agent' -type f -executable | head -n 1)" \
                "$out/bin/rslogic-agent"
              install -Dm755 "$(find target -path '*/release/rslogic-worker' -type f -executable | head -n 1)" \
                "$out/bin/rslogic-worker"
            '';

            meta = {
              description = "RsLogic client agent and RealityScan worker";
              homepage = "https://github.com/yassuh/RsLogic";
              mainProgram = "rslogic-agent";
              platforms = lib.platforms.linux;
            };
          };

          rslogic-agent = rslogic-client;
          rslogic-worker = rslogic-client;
          default = rslogic-client;
        }
      );

      checks = forAllSystems (system: {
        inherit (self.packages.${system}) rslogic-client;
      });

      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              rustc
              rustfmt
            ];
          };
        }
      );

      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixfmt-rfc-style);
    };
}
