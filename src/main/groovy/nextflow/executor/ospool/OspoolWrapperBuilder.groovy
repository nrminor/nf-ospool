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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.container.ContainerBuilder
import nextflow.executor.BashWrapperBuilder
import nextflow.executor.ScriptFileCopyStrategy
import nextflow.file.FileHelper
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun

/**
 * Bash wrapper builder for Open Science Pool (OSPool) executor
 *
 * Extends BashWrapperBuilder to customize behavior for OSPool environments:
 * - Output unstaging control based on shared filesystem configuration
 * - Secrets path rewriting for staged directories
 * - Container bind mount path normalization
 * - Auto-staged directory bind mounts
 *
 * Note: @CompileStatic is omitted to allow dynamic method resolution for
 * container builder customization, matching the approach used in the fork.
 *
 * @author Nicholas Minor
 */
@Slf4j
class OspoolWrapperBuilder extends BashWrapperBuilder {

    /**
     * HTCondor submit file content
     */
    String manifest

    /**
     * Reference to the executor
     */
    private final OspoolExecutor executor

    /**
     * Task being executed
     */
    private final TaskRun task

    /**
     * User-configured path mappings
     */
    private final Map<String,String> pathMappings

    /**
     * Create a new OSPool wrapper builder
     *
     * @param bean Task configuration bean
     * @param task Task being executed
     * @param executor The OSPool executor instance
     * @param copyStrategy File copy strategy (typically OspoolFileCopyStrategy)
     */
    OspoolWrapperBuilder(TaskBean bean, TaskRun task, OspoolExecutor executor, ScriptFileCopyStrategy copyStrategy) {
        super(bean, copyStrategy)
        this.executor = executor
        this.task = task
        this.pathMappings = executor.config.getExecConfigProp(executor.name, 'pathMappings', null) as Map<String,String>
    }

    /**
     * Determine if outputs should be unstaged from the HTCondor sandbox.
     *
     * When not using shared filesystem, HTCondor runs in an isolated sandbox and files
     * need to be copied back to the work directory. This applies to both control files
     * (.command.out, .command.err, .command.trace) and task output files.
     *
     * NOTE: This method may be called by parent if it exists, but is not an override.
     *
     * @return true if outputs should be unstaged
     */
    protected boolean shouldUnstageOutputs() {
        final explicit = executor.config.getExecConfigProp(executor.name, 'unstageOutputs', null)
        
        if( explicit != null && explicit != 'auto' ) {
            return explicit as boolean
        }
        
        return !executor.isSharedFilesystem()
    }

    /**
     * Determine if control files should be unstaged.
     * Delegates to shouldUnstageOutputs() since the logic is identical.
     *
     * @return true if control files should be unstaged
     */
    protected boolean shouldUnstageControls() {
        return shouldUnstageOutputs()
    }

    /**
     * Get secrets environment variable declarations, with path rewriting for staged directories.
     *
     * If directories containing secrets have been auto-staged, this method rewrites the
     * paths in the secrets environment to point to the staged locations.
     *
     * @return Bash commands to set up secrets environment
     */
    @Override
    protected String getSecretsEnv() {
        // Get the original bash command from parent
        def command = super.getSecretsEnv()
        
        // If shared filesystem or no command, return as-is
        if( executor.isSharedFilesystem() || !command ) {
            return command
        }
        
        // Apply path normalization using autoStagedDirectories
        def normalizedCommand = command
        
        executor.getAutoStagedDirectories().each { originalPath, stagedPath ->
            // Replace any occurrence of the original path with staged path
            normalizedCommand = normalizedCommand.replace(originalPath.toString(), stagedPath.toString())
        }
        
        if( normalizedCommand != command ) {
            log.trace "[OSPOOL] Rewritten secrets command for task ${task.name}"
            log.trace "[OSPOOL]   Original: ${command}"
            log.trace "[OSPOOL]   Rewritten: ${normalizedCommand}"
        }
        
        return normalizedCommand
    }

    /**
     * Create a container builder with OSPool-specific customizations:
     * - Path normalization for input file bind mounts
     * - Bind mounts for auto-staged directories
     * - Complete container environment configuration
     *
     * This implementation ensures that containers can properly access:
     * - Input files through normalized (canonical) paths
     * - Auto-staged directories mounted at their original paths
     * - Proper environment variables and resource limits
     *
     * @param changeDir The directory to change to
     * @return ContainerBuilder with OSPool customizations
     */
    @Override
    protected ContainerBuilder createContainerBuilder(String changeDir) {
        // If shared filesystem, use default implementation
        if( executor.isSharedFilesystem() ) {
            return super.createContainerBuilder(changeDir)
        }
        
        // For non-shared filesystem, create a custom container builder
        final ContainerBuilder builder = createContainerBuilder0()
        
        // 1. Normalize input files for container bind mounts
        // This ensures symlinked paths are converted to canonical paths that containers can mount
        if( stageInMode != 'copy' && allowContainerMounts ) {
            final normalizedInputFiles = new LinkedHashMap<String,Path>()
            inputFiles.each { stageName, storePath ->
                def normalized = executor.normalizePathWithStaging(
                    ((Path)storePath).toAbsolutePath().toString(), 
                    pathMappings
                )
                normalizedInputFiles[(String)stageName] = Paths.get(normalized)
            }
            builder.addMountForInputs(normalizedInputFiles)
        }
        
        // 2. Add standard mounts
        if( allowContainerMounts )
            builder.addMounts(binDirs)
        
        if( this.containerMount )
            builder.addMount(containerMount)
        
        // 3. Add bind mounts for all auto-staged directories
        // This mounts the staged location back to the original path inside the container
        // Example: if /home/user/project was staged to /staging/work/.staged-project,
        // we mount /staging/work/.staged-project:/home/user/project in the container
        if( executor.getAutoStagedDirectories() ) {
            executor.getAutoStagedDirectories().each { originalPath, stagedPath ->
                log.trace "[OSPOOL] Adding auto-staged bind mount: ${stagedPath} -> ${originalPath}"
                builder.addRunOptions("-B ${stagedPath}:${originalPath}")
            }
        }
        
        // 4. Configure container environment
        if( allowContainerMounts )
            builder.setWorkDir(workDir)
        
        builder.setName('$NXF_BOXID')
        
        if( this.containerMemory )
            builder.setMemory(containerMemory)
        
        if( this.containerCpus )
            builder.setCpus(containerCpus)
        
        if( this.containerCpuset )
            builder.addRunOptions(containerCpuset)
        
        if( this.containerPlatform )
            builder.setPlatform(this.containerPlatform)
        
        // 5. Add environment variables
        builder.addEnv('NXF_TASK_WORKDIR')
        
        if( isTraceRequired() )
            builder.addEnv('NXF_DEBUG=${NXF_DEBUG:=0}')
        
        if( fixOwnership() )
            builder.addEnv('NXF_OWNER=$(id -u):$(id -g)')
        
        for( String var : containerConfig.getEnvWhitelist() ) {
            builder.addEnv(var)
        }
        
        if( !isSecretNative() && secretNames ) {
            for( String var : secretNames )
                builder.addEnv(var)
        }
        
        // 6. Configure temp directory and other params
        if( containerConfig.getTemp() == 'auto' )
            builder.setTemp( changeDir ? '$NXF_SCRATCH' : '$(nxf_mktemp)' )
        
        if( containerConfig.getKill() != null )
            builder.params(kill: containerConfig.getKill())
        
        if( containerConfig.entrypointOverride() )
            builder.params(entry: '/bin/bash')
        
        if( containerOptions ) {
            builder.addRunOptions(containerOptions)
        }
        
        builder.addMountWorkDir( changeDir as boolean || FileHelper.getWorkDirIsSymlink() )
        
        builder.build()
        return builder
    }

    /**
     * Build the wrapper script and HTCondor submit file.
     *
     * @return Path to the generated wrapper script
     */
    @Override
    Path build() {
        final wrapper = super.build()
        wrapper.setExecutable(true)
        
        final condorFile = executor.resolveSubmitFilePath(task)
        
        // Create parent directories if needed
        if( condorFile.parent != this.workDir ) {
            Files.createDirectories(condorFile.parent)
        }
        
        // Write HTCondor submit file
        condorFile.text = manifest
        
        return wrapper
    }
}
