{
  description = "Nextflow plugin development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        # Java version - supporting both 17 and 21 as per CI config
        jdk = pkgs.jdk21;

      in
      {
        devShells.default = pkgs.mkShell {
          name = "nf-osp-dev";

          buildInputs = with pkgs; [
            # Java Development Kit
            jdk

            # Gradle (the wrapper will use this or download its own)
            gradle

            # Nextflow (will use system Java via NXF_JAVA_HOME)
            nextflow

            # Groovy (for Groovy development)
            groovy

            # Build tools
            gnumake

            # Version control
            git

            # Shell utilities that might be needed
            coreutils
            findutils
            gnused
            gnugrep
            gawk

            # Compression utilities (often needed by Gradle)
            gzip
            unzip
            zip

            # Networking tools (for downloading dependencies)
            curl
            wget

            # Process management
            procps

            # Text processing
            less

            # For direnv integration
            direnv

            # Additional cute stuff
            fzf-make
            bat
            ripgrep
            opencode
            btop
            lazygit
          ];

          shellHook = ''
            # Set JAVA_HOME explicitly
            export JAVA_HOME="${jdk}"
            export PATH="$JAVA_HOME/bin:$PATH"

            # Gradle configuration
            export GRADLE_USER_HOME="$PWD/.gradle-cache"
            export GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.home=$JAVA_HOME"

            # Nextflow configuration
            export NXF_HOME="$PWD/.nextflow-cache"
            export NXF_JAVA_HOME="$JAVA_HOME"

            # Ensure gradlew is executable
            if [ -f ./gradlew ]; then
              chmod +x ./gradlew
            fi

            echo "================================================"
            echo "Nextflow Plugin Development Environment"
            echo "================================================"
            echo "Java version:     $(java -version 2>&1 | head -n 1)"
            echo "Gradle version:   $(gradle --version | grep Gradle | cut -d' ' -f2)"
            echo "Nextflow version: $(nextflow -version 2>&1 | head -n 1)"
            echo "Groovy version:   $(groovy --version)"
            echo "Make version:     $(make --version | head -n 1)"
            echo ""
            echo "Available commands:"
            echo "  make assemble  - Build the plugin"
            echo "  make test      - Run unit tests"
            echo "  make install   - Install plugin locally"
            echo "  make clean     - Clean build artifacts"
            echo "================================================"
          '';

          # Environment variables
          LANG = "en_US.UTF-8";
          LC_ALL = "en_US.UTF-8";

          # Prevent Gradle from using the daemon (better for Nix)
          GRADLE_OPTS = "-Dorg.gradle.daemon=false";
        };

        # Provide multiple Java versions as alternative shells
        devShells.java17 = pkgs.mkShell {
          name = "nf-osp-dev-java17";

          buildInputs = with pkgs; [
            jdk17
            gradle
            nextflow
            groovy
            gnumake
            git
            coreutils
            findutils
            gnused
            gnugrep
            gawk
            gzip
            unzip
            zip
            curl
            wget
            procps
            less
            direnv
          ];

          shellHook = ''
            export JAVA_HOME="${pkgs.jdk17}"
            export PATH="$JAVA_HOME/bin:$PATH"
            export GRADLE_USER_HOME="$PWD/.gradle-cache"
            export GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.home=$JAVA_HOME"
            export NXF_HOME="$PWD/.nextflow-cache"
            export NXF_JAVA_HOME="$JAVA_HOME"

            if [ -f ./gradlew ]; then
              chmod +x ./gradlew
            fi

            echo "Using Java 17 development environment"
            echo "Java version: $(java -version 2>&1 | head -n 1)"
          '';

          LANG = "en_US.UTF-8";
          LC_ALL = "en_US.UTF-8";
          GRADLE_OPTS = "-Dorg.gradle.daemon=false";
        };
      }
    );
}
