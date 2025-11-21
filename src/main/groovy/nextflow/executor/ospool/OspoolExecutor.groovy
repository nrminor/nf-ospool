/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.executor.ospool

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.AbstractGridExecutor
import nextflow.executor.BashWrapperBuilder
import nextflow.executor.ExecutorConfig
import nextflow.executor.GridTaskHandler
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun
import nextflow.secret.LocalSecretsProvider
import nextflow.secret.SecretsLoader
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

/**
 * Executor for Open Science Pool (OSPool) HTCondor environment
 *
 * Implements HTCondor job submission with specific support for OSPool constraints:
 * - Submit file location restrictions (cannot submit from /staging)
 * - Non-shared filesystem execution with isolated sandboxes
 * - Path normalization for symlinked filesystems
 * - Automatic staging of inaccessible directories
 *
 * @author Nicholas Minor
 */
@Slf4j
@ServiceName('ospool')
@CompileStatic
class OspoolExecutor extends AbstractGridExecutor implements ExtensionPoint {

    static final public String CMD_CONDOR = '.command.condor'
    static final public String CMD_CONDOR_LOG = '.condor.log'

    /**
     * Staged bin directory path (populated when bin directory staging is needed)
     */
    private Path stagedBinDir

    /**
     * Map of auto-staged directories: original path -> staged path
     * This is populated during register() if directories need to be staged
     */
    private Map<String, Path> autoStagedDirectories = [:]

    /**
     * Get the auto-staged directories map.
     * 
     * @return Map of original path -> staged path
     */
    Map<String, Path> getAutoStagedDirectories() {
        return autoStagedDirectories
    }

    /**
     * Resolves the path where the HTCondor submit file should be written.
     * When submitFileDir is configured, writes to that location instead of the task work directory.
     * This allows submission from allowed locations (e.g., home directory) when work directory
     * is on a restricted filesystem (e.g., /staging at CHTC).
     *
     * The submit file directory structure mirrors the work directory structure:
     * - work/XX/XXYYYY.../.command.run
     * - submitFileDir/XX/XXYYYY.../.command.condor
     *
     * @param task The task being submitted
     * @return Path to the submit file location
     */
    protected Path resolveSubmitFilePath(TaskRun task) {
        final submitDir = config.getExecConfigProp(name, 'submitFileDir', null) as String
        
        if( submitDir ) {
            final submitBaseDir = Paths.get(submitDir)
            final taskHash = task.workDir.name
            final prefix = task.workDir.parent.name
            return submitBaseDir.resolve(prefix).resolve(taskHash).resolve(CMD_CONDOR)
        } else {
            return task.workDir.resolve(CMD_CONDOR)
        }
    }

    /**
     * Resolves the path where the HTCondor log file should be written.
     * Follows the same logic as submit file path resolution.
     *
     * The log file directory structure mirrors the work directory structure:
     * - work/XX/XXYYYY.../.command.log (Nextflow task log)
     * - submitFileDir/XX/XXYYYY.../.condor.log (HTCondor event log)
     *
     * @param task The task being submitted
     * @return Path to the log file location
     */
    protected Path resolveLogFilePath(TaskRun task) {
        final submitDir = config.getExecConfigProp(name, 'submitFileDir', null) as String
        
        if( submitDir ) {
            final submitBaseDir = Paths.get(submitDir)
            final taskHash = task.workDir.name
            final prefix = task.workDir.parent.name
            return submitBaseDir.resolve(prefix).resolve(taskHash).resolve(CMD_CONDOR_LOG)
        } else {
            return task.workDir.resolve(CMD_CONDOR_LOG)
        }
    }

    /**
     * Returns whether the executor uses a shared filesystem.
     * Default is false for OSPool (isolated sandbox execution).
     *
     * @return true if shared filesystem, false otherwise
     */
    protected boolean isSharedFilesystem() {
        config.getExecConfigProp(name, 'sharedFilesystem', false) as boolean
    }

    /**
     * Creates a task handler for the given task.
     * Returns a custom OspoolTaskHandler that overrides ProcessBuilder creation
     * to avoid setting the working directory.
     *
     * @param task The task to be executed
     * @return OspoolTaskHandler instance
     */
    @Override
    GridTaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir
        log.trace "[OSPOOL] Launching process > ${task.name} -- work folder: ${task.workDirStr}"
        return new OspoolTaskHandler(task, this)
    }

    /**
     * Normalize a path by applying auto-staged directory mappings and user-configured path mappings.
     *
     * This method provides consistent path rewriting across multiple components:
     * - File copy strategy (for staging symlinks)
     * - Container builder (for bind mounts)
     * - Secrets handling (for path rewriting)
     *
     * The normalization process applies two transformations in order:
     *
     * 1. Auto-staged directories (Nextflow-internal):
     *    If a directory was staged from an inaccessible location to an accessible one,
     *    paths within that directory are rewritten to point to the staged location.
     *    Example: /home/user/project/script.sh -> /staging/.../work/.staged-project/script.sh
     *
     * 2. User-configured path mappings (external system topology):
     *    Canonical filesystem paths are mapped to normalized paths for container bind mounts.
     *    Example: /mnt/htc-cephfs/fuse/root/staging/file.txt -> /staging/file.txt
     *
     * Paths are sorted by length (longest first) to handle nested paths correctly.
     *
     * @param path The path to normalize
     * @param pathMappings Optional user-configured path mappings
     * @return The normalized path with staging and mappings applied
     */
    protected String normalizePathWithStaging(String path, Map<String,String> pathMappings = null) {
        // FIRST: Check auto-staged directories (Nextflow-internal staging)
        // Sort by key length (longest first) to handle nested paths correctly
        if( autoStagedDirectories ) {
            def sortedStaged = autoStagedDirectories.entrySet()
                .sort { a, b -> b.key.length() <=> a.key.length() }
            
            for( entry in sortedStaged ) {
                if( path.startsWith(entry.key) ) {
                    def normalized = entry.value.toString() + path.substring(entry.key.length())
                    log.trace "[OSPOOL] Auto-staged path rewrite: $path -> $normalized"
                    return normalized
                }
            }
        }
        
        // SECOND: Apply user-configured path mappings (external system topology)
        if( !pathMappings ) {
            return path
        }
        
        def sortedMappings = pathMappings.entrySet()
            .sort { a, b -> b.key.length() <=> a.key.length() }
        
        for( entry in sortedMappings ) {
            if( path.startsWith(entry.key) ) {
                def normalized = entry.value + path.substring(entry.key.length())
                log.trace "[OSPOOL] Path mapping applied: $path -> $normalized"
                return normalized
            }
        }
        
        // No transformation needed
        return path
    }

    /**
     * Check if a path is accessible from compute nodes.
     *
     * In OSPool/CHTC environments, only certain filesystem paths are accessible from
     * compute nodes. This method checks if a path starts with a known accessible prefix.
     *
     * Default accessible prefixes:
     * - /staging (CHTC shared storage)
     * - /cvmfs (CVMFS distributed filesystem)
     * - /mnt/gluster (Gluster shared storage)
     *
     * @param path The path to check
     * @return true if accessible from compute nodes, false otherwise
     */
    protected boolean isAccessibleFromComputeNodes(Path path) {
        final pathStr = path.toAbsolutePath().toString()
        final accessiblePrefixes = ['/staging', '/cvmfs', '/mnt/gluster']
        
        return accessiblePrefixes.any { pathStr.startsWith(it) }
    }

    /**
     * Stage a directory from an inaccessible location to an accessible one.
     *
     * Creates a hidden staging directory in the work directory and copies the source
     * directory contents (excluding hidden files and common directories like .git, work, etc.).
     *
     * Special handling for secrets directory: only copies .nf-*.secrets files with
     * proper permissions (rw-------).
     *
     * @param sourceDir The source directory to stage
     * @return Path to the staged directory
     */
    protected Path stageDirectory(Path sourceDir) {
        try {
            def dirName = sourceDir.fileName.toString()
            def stagingDirName = ".staged-${dirName}"
            def targetDir = session.workDir.resolve(stagingDirName)
            
            Files.createDirectories(targetDir)
            
            // Special handling for secrets (hidden files with specific pattern)
            if( dirName == 'secrets' && sourceDir.parent?.fileName?.toString() == '.nextflow' ) {
                log.debug "[OSPOOL] Staging secrets directory: ${sourceDir} -> ${targetDir}"
                sourceDir.eachFile { file ->
                    if( file.name.startsWith('.nf-') && file.name.endsWith('.secrets') ) {
                        final target = targetDir.resolve(file.name)
                        Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING)
                        target.setPermissions('rw-------')
                        log.trace "[OSPOOL] Staged secret file: ${file.name}"
                    }
                }
            } else {
                // Normal directory staging (excludes hidden files)
                log.debug "[OSPOOL] Staging directory: ${sourceDir} -> ${targetDir}"
                copyDirectoryTree(sourceDir, targetDir)
            }
            
            log.debug "[OSPOOL] Successfully staged directory to ${targetDir}"
            return targetDir
            
        } catch( Exception e ) {
            log.error "[OSPOOL] Failed to stage directory ${sourceDir}: ${e.message}"
            throw new IllegalStateException(
                "Failed to stage directory for OSPool execution. " +
                "Directory (${sourceDir}) is not accessible from compute nodes. " +
                "Either move it to /staging or ensure it's in an accessible location.",
                e
            )
        }
    }

    /**
     * Copy directory tree recursively, excluding unwanted files and directories.
     *
     * Exclusions:
     * - Hidden files and directories (starting with '.')
     * - Common work directories: work, results, logs, .git, .nextflow
     *
     * @param source Source directory
     * @param target Target directory
     */
    protected void copyDirectoryTree(Path source, Path target) {
        final excludedDirs = ['work', 'results', 'logs', '.git', '.nextflow'] as Set
        
        source.eachFileRecurse { srcFile ->
            final relativePath = source.relativize(srcFile)
            
            // Skip if any component in the path starts with '.' (hidden)
            if( relativePath.any { it.toString().startsWith('.') } ) {
                return
            }
            
            // Skip if path starts with any excluded directory
            final firstComponent = relativePath.getName(0).toString()
            if( excludedDirs.contains(firstComponent) ) {
                return
            }
            
            final targetFile = target.resolve(relativePath)
            
            if( srcFile.isDirectory() ) {
                Files.createDirectories(targetFile)
            } else {
                Files.createDirectories(targetFile.parent)
                Files.copy(srcFile, targetFile, StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }

    /**
     * Process auto-staging directories.
     *
     * Determines which directories need to be staged (either from user config or auto-detected)
     * and stages them to accessible locations. Updates the autoStagedDirectories map.
     *
     * Auto-detection includes:
     * - Project directory (session.baseDir) if not accessible
     * - Secrets directory if enabled and not accessible
     */
    protected void processAutoStageDirectories() {
        // Get user-configured directories or build default list
        def autoStageList = config.getExecConfigProp(name, 'autoStageDirectories', null) as List
        
        if( autoStageList == null ) {
            autoStageList = []
            
            // Stage projectDir if not accessible
            if( session.baseDir && !isAccessibleFromComputeNodes(session.baseDir) ) {
                log.debug "[OSPOOL] Project directory not accessible, will stage: ${session.baseDir}"
                autoStageList << session.baseDir.toString()
            }
            
            // Stage secrets directory if enabled and not accessible
            if( SecretsLoader.isEnabled() ) {
                def provider = SecretsLoader.instance.load()
                if( provider instanceof LocalSecretsProvider ) {
                    def secretsDir = provider.@storeFile?.parent
                    if( secretsDir && !isAccessibleFromComputeNodes(secretsDir) ) {
                        log.debug "[OSPOOL] Secrets directory not accessible, will stage: ${secretsDir}"
                        autoStageList << secretsDir.toString()
                    }
                }
            }
        }
        
        // Check if any directories need staging
        if( !autoStageList ) {
            log.debug "[OSPOOL] No directories configured for auto-staging"
            return
        }
        
        log.debug "[OSPOOL] Processing autoStageDirectories: ${autoStageList}"
        
        // Stage each directory in the list
        for( dirSpec in autoStageList ) {
            final sourceDir = resolveDirectoryPath(dirSpec)
            
            if( !sourceDir ) {
                log.warn "[OSPOOL] Could not resolve directory: ${dirSpec}"
                continue
            }
            
            if( isAccessibleFromComputeNodes(sourceDir) ) {
                log.debug "[OSPOOL] Directory already accessible, skipping: ${sourceDir}"
                continue
            }
            
            if( sourceDir.exists() ) {
                final stagedDir = stageDirectory(sourceDir)
                autoStagedDirectories[sourceDir.toAbsolutePath().toString()] = stagedDir
                log.debug "[OSPOOL] Staged directory: ${sourceDir} -> ${stagedDir}"
            } else {
                log.warn "[OSPOOL] Directory to stage does not exist: ${sourceDir}"
            }
        }
        
        // Log summary of staged directories
        if( autoStagedDirectories ) {
            log.debug "[OSPOOL] Auto-staged directories summary:"
            autoStagedDirectories.each { original, staged ->
                log.debug "[OSPOOL]   ${original} -> ${staged}"
            }
        }
    }

    /**
     * Resolve a directory path specification, which may include variable references.
     *
     * Supports:
     * - Literal paths: "/path/to/dir"
     * - Variable references: "\${projectDir}", "\${baseDir}"
     * - Path objects (returned as-is)
     *
     * @param dirSpec Directory specification (String, GString, or Path)
     * @return Resolved Path, or null if resolution fails
     */
    protected Path resolveDirectoryPath(Object dirSpec) {
        if( dirSpec instanceof Path ) {
            return dirSpec as Path
        }
        
        def pathStr = dirSpec.toString()
        
        // Simple variable expansion for common cases
        pathStr = pathStr.replace('${projectDir}', session.baseDir?.toString() ?: '')
        pathStr = pathStr.replace('${baseDir}', session.baseDir?.toString() ?: '')
        
        if( pathStr.isEmpty() ) {
            return null
        }
        
        return Paths.get(pathStr)
    }

    /**
     * Override getBinDir to support bin directory staging.
     *
     * When sharedFilesystem is false, stages the project bin directory to the work directory
     * so it's accessible from compute nodes.
     *
     * @return Path to bin directory (staged or original)
     */
    @Override
    Path getBinDir() {
        final explicit = config.getExecConfigProp(name, 'stageBinDir', null)
        
        final shouldStage = (explicit == null || explicit == 'auto')
            ? !isSharedFilesystem()
            : (explicit as boolean)
        
        if( !shouldStage ) {
            return session.getBinDir()
        }
        
        final projectBinDir = session.getBinDir()
        if( !projectBinDir ) {
            return null
        }
        
        if( stagedBinDir ) {
            return stagedBinDir
        }
        
        synchronized(this) {
            if( stagedBinDir ) {
                return stagedBinDir
            }
            
            final targetDir = session.workDir.resolve('.nextflow-bin')
            
            Files.createDirectories(targetDir)
            
            projectBinDir.eachFile { file ->
                final target = targetDir.resolve(file.name)
                if( file.isDirectory() ) {
                    file.copyTo(target)
                } else {
                    Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING)
                    target.setPermissions(file.getPermissions())
                }
            }
            
            log.info "[OSPOOL] Staged bin directory: ${projectBinDir} -> ${targetDir}"
            stagedBinDir = targetDir
            return stagedBinDir
        }
    }

    /**
     * Shutdown executor and cleanup submit files if configured.
     * 
     * This method is called when the workflow completes. If session cleanup
     * is enabled, it will remove the submitFileDir directory to clean up
     * accumulated submit files.
     */
    @Override
    void shutdown() {
        super.shutdown()
        if( session.config.cleanup )
            cleanupSubmitFiles()
    }

    /**
     * Cleanup HTCondor submit files from the submitFileDir.
     * 
     * This method removes the submitFileDir directory when the workflow completes,
     * preventing accumulation of submit files over multiple workflow runs.
     */
    protected void cleanupSubmitFiles() {
        final submitDir = config.getExecConfigProp(name, 'submitFileDir', null) as String
        
        if( submitDir ) {
            final submitBaseDir = Paths.get(submitDir)
            
            if( submitBaseDir.exists() ) {
                log.debug "[OSPOOL] Cleaning up submit files in: $submitBaseDir"
                try {
                    submitBaseDir.deleteDir()
                } catch( Exception e ) {
                    log.warn "[OSPOOL] Failed to cleanup submit files: ${e.message}"
                }
            }
        }
    }

    /**
     * Initialize executor and process auto-staging.
     */
    @Override
    protected void register() {
        super.register()
        
        // Validate configuration when not using shared filesystem
        if( !isSharedFilesystem() ) {
            final submitDir = config.getExecConfigProp(name, 'submitFileDir', null)
            
            if( !submitDir ) {
                final errorMsg = """\
                    OSPool executor configuration error:
                    
                    When sharedFilesystem = false, submitFileDir must be specified.
                    
                    Example configuration:
                    executor {
                        \$ospool {
                            submitFileDir = '.nextflow/ospool-submit'
                            pathMappings = [
                                '/mnt/htc-cephfs/fuse/root/staging': '/staging'
                            ]
                        }
                    }
                    """.stripIndent()
                
                throw new IllegalArgumentException(errorMsg)
            }
            
            // Log configuration details
            log.debug "[OSPOOL] Running in restricted filesystem mode - submit files: ${submitDir}"
            
            final pathMappings = config.getExecConfigProp(name, 'pathMappings', null)
            log.debug "[OSPOOL] pathMappings raw value: ${pathMappings}, type: ${pathMappings?.getClass()?.name}"
            
            if( pathMappings && pathMappings instanceof Map ) {
                final mappingsMap = pathMappings as Map<String,String>
                if( mappingsMap && !mappingsMap.isEmpty() ) {
                    log.debug "[OSPOOL] Path mappings configured:"
                    mappingsMap.each { canonical, alias ->
                        log.debug "[OSPOOL]   ${canonical} -> ${alias}"
                    }
                } else {
                    log.debug "[OSPOOL] Path mappings is empty map"
                }
            } else {
                log.debug "[OSPOOL] No path mappings configured - canonical paths will NOT be normalized"
            }
            
            log.debug "[OSPOOL] Input staging: ${pathMappings ? 'symlink (with path normalization)' : 'symlink'}"
            log.debug "[OSPOOL] Output unstaging: enabled"
            
            // Process auto-staging directories
            processAutoStageDirectories()
        } else {
            log.debug "[OSPOOL] Running in shared filesystem mode"
        }
    }

    /**
     * Create a custom bash wrapper builder for OSPool tasks.
     * 
     * Returns an OspoolWrapperBuilder that handles:
     * - Output unstaging control based on shared filesystem configuration
     * - Secrets path rewriting for staged directories
     * - Container bind mount path normalization
     * - Auto-staged directory bind mounts
     *
     * @param task The task to create a wrapper for
     * @return BashWrapperBuilder instance (OspoolWrapperBuilder)
     */
    @Override
    BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        final bean = new TaskBean(task)
        final pathMappings = config.getExecConfigProp(name, 'pathMappings', null) as Map<String,String>
        final copyStrategy = new OspoolFileCopyStrategy(bean, pathMappings, this)
        final builder = new OspoolWrapperBuilder(bean, task, this, copyStrategy)
        
        // Generate HTCondor submit file directives and store in manifest
        builder.manifest = getDirectivesText(task)
        
        return builder
    }

    /**
     * Format HTCondor directives as text for the submit file.
     *
     * Calls getDirectives() to generate the directive list and formats
     * them as a submit file with one directive per line.
     *
     * @param task The task to generate directives for
     * @return Formatted HTCondor submit file content
     */
    protected String getDirectivesText(TaskRun task) {
        final List<String> directives = []
        getDirectives(task, directives)
        return directives.join('\n') + '\n'
    }

    //
    // Abstract method implementations from AbstractGridExecutor
    //

    @Override
    protected String getHeaderToken() {
        throw new UnsupportedOperationException("OSPool executor does not use header tokens")
    }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> result) {
        // HTCondor submit file directives
        result << "universe = vanilla"
        result << "executable = ${task.workDir.resolve(TaskRun.CMD_RUN)}".toString()
        
        final logPath = resolveLogFilePath(task)
        result << "log = ${logPath}".toString()
        
        // getenv controls whether to inherit environment from submit node
        final defaultGetenv = isSharedFilesystem()
        final getenv = config.getExecConfigProp(name, 'getenv', defaultGetenv) as boolean
        if( getenv ) {
            result << "getenv = true"
        }

        // Resource requests
        if( task.config.getCpus() > 1 ) {
            result << "request_cpus = ${task.config.getCpus()}".toString()
            result << "machine_count = 1"
        }

        if( task.config.getMemory() ) {
            result << "request_memory = ${task.config.getMemory()}".toString()
        }

        if( task.config.getDisk() ) {
            result << "request_disk = ${task.config.getDisk()}".toString()
        }

        if( task.config.getTime() ) {
            result << "periodic_remove = (RemoteWallClockTime - CumulativeSuspensionTime) > ${task.config.getTime().toSeconds()}".toString()
        }

        // Cluster options (e.g., HasCHTCStaging=true for CHTC)
        if( task.config.getClusterOptions() ) {
            def opts = task.config.getClusterOptions()
            if( opts instanceof Collection ) {
                result.addAll(opts as Collection)
            }
            else {
                result.addAll( opts.toString().tokenize(';\n').collect{ it.trim() })
            }
        }

        result << "queue"
        
        return result
    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        final condorFile = resolveSubmitFilePath(task)
        return ['condor_submit', '--terse', condorFile.toString()]
    }

    @Override
    def parseJobId(String text) {
        text.tokenize(' -')[0]
    }

    @Override
    protected List<String> getKillCommand() {
        ['condor_rm']
    }

    @Override
    protected List<String> queueStatusCommand(Object queue) {
        ["condor_q", "-nobatch"]
    }

    static protected Map<String, QueueStatus> DECODE_STATUS = [
            'U': QueueStatus.PENDING,   // Unexpanded
            'I': QueueStatus.PENDING,   // Idle
            'R': QueueStatus.RUNNING,   // Running
            'X': QueueStatus.ERROR,     // Removed
            'C': QueueStatus.DONE,      // Completed
            'H': QueueStatus.HOLD,      // Held
            'E': QueueStatus.ERROR      // Error
    ]

    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {
        final result = new LinkedHashMap<String, QueueStatus>()
        if( !text ) return result

        boolean started = false
        def itr = text.readLines().iterator()
        while( itr.hasNext() ) {
            String line = itr.next()
            if( !started ) {
                started = line.startsWith(' ID ')
                continue
            }

            if( !line.trim() ) {
                break
            }

            def cols = line.tokenize(' ')
            def id = cols[0]
            def st = cols[5]
            result[id] = DECODE_STATUS[st]
        }

        return result
    }
}
