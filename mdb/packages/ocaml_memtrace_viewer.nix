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
    sha256 = "sha256-vtqhN+ro9UUqmgt8mxNq3caAvtjBr2YVFFvrYF22JoE=";
  };

  propagatedBuildInputs = (with pkgs; [
    libunwind
  ]) ++ (with ocamlPackages; [
    memtrace
    ctypes
    dune-configurator
    ounit2
    js_of_ocaml
    js_of_ocaml-ppx
    janeStreet.ocaml-embed-file
    bonsai
  ]);

  doCheck = true;
}
