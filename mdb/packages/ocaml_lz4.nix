{ pkgs, ... }:
let
  ocamlPackages = pkgs.ocamlPackages;
in
ocamlPackages.buildDunePackage rec {
  pname = "lz4";
  name = pname;
  minimalOCamlVersion = "4.02";
  version = "1.3.0";
  src = pkgs.fetchzip {
    url = "https://github.com/whitequark/ocaml-lz4/releases/download/v${version}/lz4-${version}.tbz";
    sha256 = "sha256-x95lpdHtry5K0H/hXjmacJ+3ymxpw60ek++MSRhKSuU=";
  };

  propagatedBuildInputs = (with pkgs; [
    lz4
  ]) ++ (with ocamlPackages; [
    ctypes
    dune-configurator
    ounit2
  ]);

  doCheck = true;
}
