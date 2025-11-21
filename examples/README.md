# nf-ospool Examples

This directory contains working examples demonstrating how to use the nf-ospool plugin with different configurations.

**⚠️ Important:** These examples require an HTCondor environment (such as UW-Madison CHTC or OSPool access points) to run successfully. They will not work in standard environments without HTCondor installed.

## Prerequisites

1. Access to an HTCondor submit node (e.g., CHTC submit server)
2. Nextflow installed (version >= 25.10.0)
3. The nf-ospool plugin installed
4. Access to `/staging` shared storage (for CHTC users)

## Examples

### 1. Basic Example (`basic/`)

**What it demonstrates:**
- Minimal configuration to use the OSPool executor
- Simple process execution across multiple tasks
- Basic HTCondor submit file generation

**How to run:**
```bash
cd basic
# Edit nextflow.config to set your work directory
nextflow run main.nf
```

**Key concepts:**
- `process.executor = 'ospool'` - Use OSPool for all processes
- `submitFileDir` - Where HTCondor submit files are stored (must NOT be in /staging)
- `workDir` - Where task work directories are created (must be in /staging for CHTC)

---

### 2. With Containers (`with-containers/`)

**What it demonstrates:**
- Using Docker/Singularity containers with OSPool
- Container image pulling and caching
- Automatic bind mount configuration

**How to run:**
```bash
cd with-containers
# Edit nextflow.config to set your work directory
nextflow run main.nf
```

**Key concepts:**
- `singularity.enabled = true` - Enable Singularity container support
- Containers are automatically converted from Docker format
- The plugin handles bind mounts for /staging and input files

---

### 3. Directory Staging (`staging/`)

**What it demonstrates:**
- Automatic staging of files from inaccessible locations
- Path mappings for container bind mounts
- Using project scripts with OSPool jobs

**How to run:**
```bash
cd staging
# Edit nextflow.config to set your directories
nextflow run main.nf
```

**Key concepts:**
- `autoStageDirectories` - Directories to automatically stage to /staging
- `pathMappings` - Normalize paths for container bind mounts
- Useful when your project code is in /home but jobs run with /staging access

**When to use directory staging:**
- Your project/scripts are in a location not accessible from compute nodes (e.g., `/home`)
- You want to use local scripts without manually copying them
- You need consistent paths between submit and execution nodes

---

## Configuration Reference

### Common Settings

```groovy
plugins {
    id 'nf-ospool@0.1.0'
}

process {
    executor = 'ospool'
}

executor {
    $ospool {
        // Required: Where to write HTCondor submit files
        // Cannot be in /staging (HTCondor restriction)
        submitFileDir = "${HOME}/.nextflow/ospool-submit"
        
        // Optional: Directories to auto-stage (not accessible from compute nodes)
        autoStageDirectories = ['/home']
        
        // Optional: Path normalization for containers
        pathMappings = [
            '/mnt/htc-cephfs/fuse/root/staging': '/staging'
        ]
        
        // Optional: Use shared filesystem mode (default: false)
        sharedFilesystem = false
    }
}

// Required: Work directory on shared storage
workDir = '/staging/groups/YOUR_GROUP/nextflow-work'
```

### For CHTC Users

Replace `YOUR_GROUP` with your actual CHTC group name:
```groovy
workDir = '/staging/groups/YOUR_GROUP/nextflow-work'
```

## Testing Your Setup

After configuring an example for your environment, test that the plugin loads correctly:

```bash
cd basic
nextflow config
```

You should see:
```
OSPool executor plugin started
```

## Troubleshooting

### "Cannot submit from /staging"
- **Problem:** `submitFileDir` is set to a path in `/staging`
- **Solution:** Use a path in your home directory: `${HOME}/.nextflow/ospool-submit`

### "Work directory not accessible"
- **Problem:** `workDir` is not on shared storage
- **Solution:** Use a path in `/staging/groups/YOUR_GROUP/`

### "Container not found"
- **Problem:** Singularity can't pull the container
- **Solution:** Pre-pull containers or use a container cache in `/staging`

### Jobs stay in queue but don't run
- **Problem:** Resource requirements might be too restrictive
- **Solution:** Check HTCondor logs in `<submitFileDir>/<taskHash>/.condor.log`

## Next Steps

1. **Start with `basic/`** to verify your setup works
2. **Try `with-containers/`** if you use Docker/Singularity
3. **Use `staging/`** as a template for projects with complex file access patterns

## Documentation

For complete documentation, see the main [README.md](../README.md) in the repository root.

For OSPool-specific guidance, visit: https://portal.osg-htc.org/documentation/
