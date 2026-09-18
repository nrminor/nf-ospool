package nextflow.executor.ospool

import java.nio.file.Files
import java.nio.file.Path

import nextflow.Global
import nextflow.Session
import nextflow.container.SingularityConfig
import nextflow.executor.ExecutorConfig
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.TempDir

class OspoolWrapperBuilderTest extends Specification {

    @TempDir
    Path tempDir

    def 'should use mapped paths for container input mounts'() {
        given:
        def canonicalPrefix = '/mnt/htc-cephfs/kernel/root/staging'
        def canonicalInput = Path.of("${canonicalPrefix}/groups/example/reads/sample.fastq.gz")
        def pathMappings = [(canonicalPrefix): '/staging']
        def workDir = tempDir.resolve('work/ab/abcdef')
        def submitDir = tempDir.resolve('submit')
        Files.createDirectories(workDir)
        def session = Stub(Session) {
            getWorkDir() >> workDir
            getStatsEnabled() >> false
        }
        Global.session = session

        def processor = Stub(TaskProcessor) {
            getSession() >> session
            getBinDirs() >> []
        }
        def taskConfig = new TaskConfig()
        taskConfig.stageInMode = 'symlink'

        def executor = new OspoolExecutor()
        executor.name = 'ospool'
        executor.config = Stub(ExecutorConfig) {
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
            getExecConfigProp('ospool', 'pathMappings', null) >> pathMappings
            getExecConfigProp('ospool', 'submitFileDir', null) >> submitDir.toString()
        }

        def task = Stub(TaskRun) {
            getName() >> 'mapped input'
            getWorkDir() >> workDir
            getTargetDir() >> workDir
            getScript() >> 'echo hello'
            getEnvironment() >> [:]
            getConfig() >> taskConfig
            getProcessor() >> processor
            getInputFilesMap() >> ['sample.fastq.gz': canonicalInput]
            getOutputFilesNames() >> []
            getContainer() >> 'ubuntu.sif'
            getContainerConfig() >> new SingularityConfig(enabled: true, autoMounts: true)
            isContainerEnabled() >> true
            isContainerNative() >> false
            isSecretNative() >> false
        }
        def builder = executor.createBashWrapperBuilder(task)

        when:
        def wrapper = Files.readString(builder.build())
        def launchLine = wrapper.readLines().find { it.contains('singularity ') }

        then:
        launchLine.contains('-B /staging/groups/example/reads')
        !launchLine.contains("-B ${canonicalPrefix}/groups/example/reads")
        wrapper.contains('ln -s /staging/groups/example/reads/sample.fastq.gz sample.fastq.gz')
        !wrapper.contains("ln -s ${canonicalInput} sample.fastq.gz")

        cleanup:
        Global.session = null
    }
}
