# nf-ospool

Nextflow plugin for the Open Science Pool (OSPool) HTCondor environment.

## Overview

This plugin provides a custom Nextflow executor (`ospool`) specifically designed for the Open Science Pool HTCondor environment at UW-Madison CHTC and similar infrastructures. It addresses unique constraints in OSPool environments:

- **Submit location restrictions**: HTCondor policy blocks `condor_submit` from running in certain directories (e.g., `/staging/`)
- **Non-shared filesystem**: Compute nodes use isolated sandboxes with different filesystem topology than submit nodes
- **Path normalization**: Symlinked paths must be normalized for container bind mounts
- **Directory staging**: Project directories and secrets may not be accessible from compute nodes

## Quick Start

### Installation

Add the plugin to your Nextflow configuration:

```groovy
plugins {
    id 'nf-ospool@0.1.0'
}
```

### Basic Configuration

```groovy
// nextflow.config

plugins {
    id 'nf-ospool@0.1.0'
}

process {
    executor = 'ospool'
    
    // OSPool-specific cluster options
    clusterOptions = 'HasCHTCStaging=true'
}

executor {
    $ospool {
        // Required: where to write submit files (not on /staging)
        submitFileDir = '.nextflow/ospool-submit'
        
        // Path normalization for symlinked filesystems
        pathMappings = [
            '/mnt/htc-cephfs/fuse/root/staging': '/staging'
        ]
    }
}

workDir = '/staging/groups/mygroup/nextflow-work'
```

## Examples

See the [examples/](examples/) directory for complete working examples:

- **[basic/](examples/basic/)** - Minimal configuration to get started
- **[with-containers/](examples/with-containers/)** - Using Docker/Singularity containers
- **[staging/](examples/staging/)** - Automatic directory staging for project files

Each example includes a complete `main.nf` and `nextflow.config` with detailed comments.

**Note:** Examples require an HTCondor environment (CHTC submit node or OSPool access point) to run.

## Configuration Options

### Executor Configuration (`executor.$ospool`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `submitFileDir` | String | Required when `sharedFilesystem=false` | Directory for HTCondor submit files |
| `sharedFilesystem` | Boolean | `false` | Whether executor uses shared filesystem |
| `pathMappings` | Map | `null` | Path mappings for symlinked filesystems |
| `autoStageDirectories` | List | Auto-detected | Directories to stage if not accessible |
| `stageBinDir` | Boolean/String | `'auto'` | Whether to stage bin directory |
| `unstageOutputs` | Boolean/String | `'auto'` | Whether to unstage outputs |
| `getenv` | Boolean | Based on `sharedFilesystem` | Inherit environment from submit node |

### Process Directives

Standard Nextflow process directives work with the OSPool executor:

```groovy
process myTask {
    cpus 4
    memory '8 GB'
    disk '10 GB'
    time '2h'
    
    clusterOptions 'HasCHTCStaging=true'
    
    """
    your_command_here
    """
}
```

## Building from Source

Build the plugin:

```bash
make assemble
```

Install locally for testing:

```bash
make install
```

Test with Nextflow:

```bash
nextflow run my-pipeline.nf -plugins nf-ospool@0.1.0
```

## Documentation

- **Examples**: See [examples/](examples/) for working examples you can run
- **Architecture**: See [PLUGIN-ARCHITECTURE.md](PLUGIN-ARCHITECTURE.md) for detailed implementation documentation
- **Nextflow Plugin Docs**: https://nextflow.io/docs/latest/plugins/developing-plugins.html
- **HTCondor Manual**: https://htcondor.readthedocs.io/
- **CHTC Documentation**: https://chtc.cs.wisc.edu/

## License

Copyright 2013-2024, Seqera Labs

Licensed under the Apache License, Version 2.0. See [COPYING](COPYING) for details.

## Credits

Developed by Nicholas Minor for UW-Madison CHTC.
