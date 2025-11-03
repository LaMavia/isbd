{ pkgs, ... }:
let
  ocamlPackages = pkgs.ocamlPackages;
in
ocamlPackages.buildDunePackage rec {
  pname = "openapi_router";
  name = pname;
  minimalOCamlVersion = "4.02";
  version = "1.3.0";
  src = pkgs.fetchFromGitHub {
    owner = "whitequark";
    repo = "ocaml-lz4";
    rev = "${version}";
    # sha256 = "sha256-HZ+UW4fTOSEHTU3awpaz4bIMC+omFMPI8fM/ZTxR7Vw=";
  };

  propagatedBuildInputs = with ocamlPackages; [
    ctypes
  ];

  doCheck = true;
}
