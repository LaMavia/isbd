{
  description = "C and C++";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        llvm = pkgs.llvmPackages_latest;
        lib = nixpkgs.lib;

      in
      {
        devShell = pkgs.mkShell rec {
          nativeBuildInputs = [
            pkgs.cmake
            llvm.lldb

            pkgs.clang-tools
            llvm.clang

            pkgs.gtest
            pkgs.gnumake
            pkgs.bear
          ];

          buildInputs = [
            llvm.libcxx
            pkgs.openssl
            pkgs.valgrind
          ];

          CPATH = builtins.concatStringsSep ":" [
            (lib.makeSearchPathOutput "dev" "include" [ llvm.libcxx pkgs.openssl ])
            (lib.makeSearchPath "resource-root/include" [ llvm.clang ])
          ];

          shellHook = ''
            # Augment the dynamic linker path
            export "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${CPATH}"
            export "LIBCLANG_PATH=${pkgs.libclang.lib}/lib";
          '';
        };
      }
    );
}
