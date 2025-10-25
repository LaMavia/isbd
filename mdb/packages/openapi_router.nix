{ pkgs, ... }:
let
  ocamlPackages = pkgs.ocamlPackages;
in
ocamlPackages.buildDunePackage rec {
  pname = "openapi";
  name = pname;
  minimalOCamlVersion = "4.07";
  version = "1.0.0";
  src = pkgs.fetchFromGitHub {
    owner = "marigold-dev";
    repo = "openapi-router";
    rev = "v${version}";
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
