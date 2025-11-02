{ pkgs, ... }:
let
  ocamlPackages = pkgs.ocamlPackages;
in
ocamlPackages.buildDunePackage rec {
  pname = "openapi_router";
  name = pname;
  minimalOCamlVersion = "4.07";
  version = "0.1.0";
  src = pkgs.fetchFromGitHub {
    owner = "marigold-dev";
    repo = "openapi-router";
    rev = "${version}";
    sha256 = "sha256-HZ+UW4fTOSEHTU3awpaz4bIMC+omFMPI8fM/ZTxR7Vw=";
  };

  propagatedBuildInputs = with ocamlPackages; [
    ppxlib
    ppx_deriving
    yojson
    ppx_yojson_conv
    ppx_yojson_conv_lib
    core
  ];

  doCheck = true;
}
