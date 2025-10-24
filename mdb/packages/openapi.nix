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
    owner = "jhuapl-saralab";
    repo = "openapi-ocaml";
    rev = "v${version}";
    sha256 = "sha256-WOskB+Q2eEaW9cOBlIwFmLh1CGYLg09BY1LePVQWbno=";
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
