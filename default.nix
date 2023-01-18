# -*- compile-command: "direnv exec . bash -c 'pip list; cabal exec -- ghc-pkg list'"; -*-
# -*- compile-command: "nix-shell --run 'cabal exec -- ghc-pkg list'"; -*-
{ sources ? import ./nix/sources.nix {} , pythonEnv ? null, pkgs ? null }:
  # https://github.com/NixOS/nixpkgs/blob/master/pkgs/development/haskell-modules/make-package-set.nix
let pkgs2 = if pkgs != null then pkgs else import sources.nixpkgs { overlays = [cabalHashes]; }; 
    cabalHashes = self: super: { inherit (sources) all-cabal-hashes; };
in
with pkgs2.haskell.lib.compose;
let pkgs = pkgs2;
    mach-nix = import sources.mach-nix {
      pkgs = import (import "${sources.mach-nix}/mach_nix/nix/nixpkgs-src.nix") {
        config = { allowUnfree = true; }; overlays = []; };
    };

    hoff = import sources.hoff {inherit sources pkgs;};

    pyEnv = if pythonEnv != null then pythonEnv else mach-nix.mkPython {
      requirements = ''
        pandas
        cbor2
        ipython
        stringcase
        sqlalchemy
        pip
        pyodbc
      '';
    };

    overrides = self: super: let
      minimalCompilation = x: disableLibraryProfiling (dontHaddock x);
      minSource = name: minimalCompilation (self.callCabal2nix name sources.${name} {});
    in hoff.overrides self super // {
      interval          = minSource "interval";
      hoff              = minSource "hoff";
      foreign-store     = minSource "foreign-store";
      # foreign-store     = self.callCabal2nix "foreign-store" /home/data/code/foreign-store {};
      date-combinators  = minSource "date-combinators";
      cache-polysemy    = doJailbreak super.cache-polysemy;
    };
    source-overrides = hoff.source-overrides
                       // {
      # polysemy          = "1.9.0.0";
      # resource-pool     = "0.4.0.0";
    };

    this = pkgs.haskellPackages.developPackage {
      root = ./.;
      withHoogle = false;
      returnShellEnv = false;
      modifier = drv:
        disableLibraryProfiling (dontHaddock (addBuildTools 
          (with pkgs.haskellPackages; [ cabal-install ghcid pyEnv]) drv));
      inherit source-overrides overrides;
    };
    mso = x: # let x = dr; #.override { openssl = pkgs.openssl_1_1; };
                "${x}/${x.driver}";
    msoPy = mso pyEnv.nixpkgs.unixODBCDrivers.msodbcsql17;
    msoH = mso pkgs.unixODBCDrivers.msodbcsql18;

    odbcini = pkgs.writeText "odbc.ini" ''
              [msodbcsql18]
              DRIVER=${msoH}
              '';

    exportEnv = ''
      export ODBCINI=${odbcini}
      export msoPy=${msoPy}
      export msoH=${msoH}
    '';
in this
   // {  inherit source-overrides overrides; # to be used by users of this package
         inherit exportEnv;
         env = this.env.overrideAttrs(_: prev: { shellHook = prev.shellHook + exportEnv; });}
