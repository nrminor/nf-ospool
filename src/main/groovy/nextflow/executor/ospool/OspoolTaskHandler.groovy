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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.GridTaskHandler
import nextflow.processor.TaskRun

/**
 * Task handler for Open Science Pool (OSPool) HTCondor executor
 *
 * Extends GridTaskHandler to customize the ProcessBuilder creation.
 * The key difference from the standard GridTaskHandler is that this implementation
 * does NOT set the working directory on the ProcessBuilder.
 *
 * This is critical for OSPool/CHTC environments where HTCondor has policy restrictions
 * that prevent condor_submit from running in certain directories (e.g., /staging).
 * By not setting the working directory, condor_submit runs from wherever the Nextflow
 * head process is located (typically the user's home directory), while the submit file
 * itself uses absolute paths to reference the actual task work directory.
 *
 * @author Nicholas Minor
 */
@Slf4j
@CompileStatic
class OspoolTaskHandler extends GridTaskHandler {

    /**
     * Create a new OSPool task handler
     *
     * @param task The task instance to be executed
     * @param executor The OSPool executor instance
     */
    OspoolTaskHandler(TaskRun task, OspoolExecutor executor) {
        super(task, executor)
    }

    /**
     * Creates a ProcessBuilder for executing the condor_submit command.
     *
     * Unlike the parent GridTaskHandler implementation, this does NOT set
     * the working directory via builder.directory(). This allows condor_submit
     * to run from the Nextflow launch directory instead of the task work directory,
     * avoiding CHTC's policy restrictions on submitting from /staging.
     *
     * The submit file uses absolute paths, so the actual execution location
     * is unaffected by where condor_submit itself runs.
     *
     * @return ProcessBuilder configured for condor_submit
     */
    @Override
    protected ProcessBuilder createProcessBuilder() {
        final cli = executor.getSubmitCommandLine(task, wrapperFile)
        log.trace "start process ${task.name} > cli: ${cli}"

        // Create ProcessBuilder WITHOUT setting directory
        // This is the key difference from GridTaskHandler - we deliberately
        // do NOT call builder.directory(task.workDir.toFile())
        return new ProcessBuilder()
            .command(cli as String[])
            .redirectErrorStream(true)
    }
}
