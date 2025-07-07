{
  inputs = {
    nixpkgs = { url = "github:NixOS/nixpkgs/nixos-unstable"; };
    nixpkgs_go_1_21 = { url = "github:NixOS/nixpkgs/?rev=fd0133851efa483a5ed8c196d74eddff0d4ccdde"; };
    systems.url = "github:nix-systems/default";
  };

  outputs = { self, nixpkgs, nixpkgs_go_1_21, systems, ... }@inputs:
    let
      eachSystem = f: nixpkgs.lib.genAttrs (import systems) f;
    in
    {
      devShells = eachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          pkgs_go_1_21 = nixpkgs_go_1_21.legacyPackages.${system};
        in
      {
        default = pkgs.mkShell {
          hardeningDisable = [ "all" ];
          buildInputs = [];

          packages = (with pkgs; [
            pkgs.gcc
            pkgs.delve
            pkgs.gotools
            pkgs.gopls
            pkgs.go-outline
            pkgs.gopkgs
            pkgs.godef
            pkgs.golangci-lint
            pkgs.go-tools
          ]) ++ (with pkgs_go_1_21; [
            pkgs_go_1_21.go_1_21
          ]);
        };
      }
    );
  };
}
