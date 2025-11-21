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

import java.nio.file.Path

import groovy.transform.CompileStatic
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.util.Escape

/**
 * File copy strategy for Open Science Pool (OSPool) executor
 *
 * This strategy extends SimpleFileCopyStrategy to apply path normalization when
 * staging input files. Path normalization is critical for OSPool/CHTC environments where:
 *
 * 1. Filesystems may be symlinked (e.g., /staging -> /mnt/htc-cephfs/fuse/root/staging)
 * 2. Container bind mounts must use the canonical path for consistency
 * 3. Compute nodes may have different filesystem topology than submit nodes
 *
 * The normalization process applies two transformations in order:
 * 1. Auto-staged directories (Nextflow-internal): directories that were staged from
 *    inaccessible locations to accessible ones
 * 2. User-configured path mappings: mappings from canonical paths to normalized paths
 *    (e.g., /mnt/htc-cephfs/fuse/root/staging -> /staging)
 *
 * @author Nicholas Minor
 */
@CompileStatic
class OspoolFileCopyStrategy extends SimpleFileCopyStrategy {

    /**
     * User-configured path mappings for normalizing canonical paths
     */
    private final Map<String,String> pathMappings

    /**
     * Reference to the executor for accessing normalization logic
     */
    private final OspoolExecutor executor

    /**
     * Create a new OSPool file copy strategy
     *
     * @param bean Task configuration bean
     * @param pathMappings User-configured path mappings (may be null)
     * @param executor The OSPool executor instance
     */
    OspoolFileCopyStrategy(TaskBean bean, Map<String,String> pathMappings, OspoolExecutor executor) {
        super(bean)
        this.pathMappings = pathMappings ?: [:]
        this.executor = executor
    }

    /**
     * Stage an input file with path normalization applied.
     *
     * This method normalizes the input file path before generating the staging command.
     * The normalization ensures that:
     * - Auto-staged directories are referenced by their staged location
     * - Symlinked paths are converted to their canonical form (for bind mounts)
     * - Container bind mounts will work correctly across different filesystem topologies
     *
     * @param path The source file path
     * @param targetName The target name in the task working directory
     * @return Bash command to stage the file
     */
    @Override
    String stageInputFile(Path path, String targetName) {
        def cmd = ''
        
        // Create parent directories if needed
        def p = targetName.lastIndexOf('/')
        if( p > 0 ) {
            cmd += "mkdir -p ${Escape.path(targetName.substring(0, p))} && "
        }
        
        // Get absolute path (handles relative paths if any)
        def pathStr = path.toAbsolutePath().toString()
        
        // Apply path normalization using executor's shared normalization logic
        // This handles both auto-staged directories and user path mappings
        pathStr = executor.normalizePathWithStaging(pathStr, pathMappings)
        
        // Generate staging command with normalized path
        cmd += stageInCommand(pathStr, targetName, stageinMode)
        
        return cmd
    }
}
