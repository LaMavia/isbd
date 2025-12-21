{ pkgs, ... }:
let
  ocamlPackages = pkgs.ocamlPackages;
in
ocamlPackages.buildDunePackage rec {
  pname = "memtrace_viewer";
  name = pname;
  minimalOCamlVersion = "5.1.0";
  version = "0.17.0";
  src = pkgs.fetchzip {
    url = "https://github.com/janestreet/memtrace_viewer/archive/refs/tags/v${version}.tar.gz";
    sha256 = "sha256-x95lpdHtry5K0H/hXjmacJ+3ymxpw60ek++MSRhKSuU=";
  };

  propagatedBuildInputs = (with pkgs; [
    libunwind
  ]) ++ (with ocamlPackages; [
    memtrace
    core
    bonsai
    ctypes
    dune-configurator
    ounit2
  ]);

  doCheck = true;
}
