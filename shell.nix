{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    sqlite
    c-ares
    grpc
    pkg-config
    zlib
    libffi
    rdkafka
    protobuf
    openssl
    gcc
    docker-compose
    clang
    (python39.withPackages(ps: with ps; [
      virtualenv
      cython
     ]))
  ];
  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [pkgs.stdenv.cc.cc ];
}

