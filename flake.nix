{
  # UNVERIFIED: nix is not installed on the machine this flake was written
  # on, so it has never been evaluated or built. Before first use, fill in
  # vendorHash: run `nix build`, let it fail on the fixed-output derivation
  # mismatch, and copy the hash from the "got: sha256-..." line over
  # lib.fakeHash below. Everything else follows the stock # NOTE (launch item, alongside the vendorHash fill): go.mod's
          # directive is go 1.25, but nixos-25.05's default buildGoModule Go
          # is 1.24 — the build will refuse. When filling vendorHash, either
          # bump the nixpkgs input past Go 1.25 or override:
          #   buildGoModule.override { go = pkgs.go_1_25; }
          buildGoModule
  # pattern for a cgo binary (mattn/go-sqlite3 compiles its bundled SQLite
  # amalgamation, so no sqlite input is needed; buildGoModule leaves
  # CGO_ENABLED=1 and stdenv's cc in place by default).
  description = "offshoot — branch SQLite like git: fork, checkpoint, rollback, promote";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      # src = self + this hardcoded version means a build from main HEAD
      # still stamps v0.2.7 until bumped — the standard in-repo-flake
      # compromise.
      version = "0.2.7";
    in
    {
      packages = forAllSystems (pkgs: rec {
        offshoot = pkgs.buildGoModule {
          pname = "offshoot";
          inherit version;
          src = self;

          # PLACEHOLDER — see the note at the top of this file.
          vendorHash = pkgs.lib.fakeHash;

          subPackages = [ "cmd/offshoot" ];

          # release.yml embeds the tag name (v-prefixed) as main.version.
          ldflags = [ "-s" "-w" "-X main.version=v${version}" ];

          # `offshoot diff` shells out to sqldiff at runtime (optional;
          # `diff --summary` needs nothing). Keep it reachable from the
          # installed binary without polluting the user's PATH.
          nativeBuildInputs = [ pkgs.makeWrapper ];
          postInstall = ''
            wrapProgram $out/bin/offshoot \
              --suffix PATH : ${pkgs.lib.makeBinPath [ pkgs.sqldiff ]}
          '';

          meta = with pkgs.lib; {
            description = "Branch SQLite like git: fork, checkpoint, rollback, promote";
            homepage = "https://github.com/sricola/offshoot";
            license = licenses.asl20;
            mainProgram = "offshoot";
            platforms = platforms.linux ++ platforms.darwin;
          };
        };
        default = offshoot;
      });
    };
}
