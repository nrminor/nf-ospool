package nextflow.executor.ospool

import nextflow.Session
import nextflow.executor.ExecutorConfig
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import spock.lang.Specification

import java.nio.file.Paths

/**
 * Tests for OspoolExecutor
 *
 * These tests verify critical path resolution logic, HTCondor directive generation,
 * and directory staging decisions.
 */
class OspoolExecutorTest extends Specification {

    def 'should resolve submit file path with submitFileDir configured'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/home/user/.nextflow/ospool-submit'
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/a1/a1b2c3d4e5f6')
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
        }
        
        when:
        def result = executor.resolveSubmitFilePath(task)
        
        then:
        result.toString() == '/home/user/.nextflow/ospool-submit/a1/a1b2c3d4e5f6/.command.condor'
    }

    def 'should resolve submit file path to work directory when no submitFileDir'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> null
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/a1/a1b2c3d4e5f6')
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
        }
        
        when:
        def result = executor.resolveSubmitFilePath(task)
        
        then:
        result.toString() == '/staging/work/a1/a1b2c3d4e5f6/.command.condor'
    }

    def 'should resolve log file path with submitFileDir configured'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/home/user/.nextflow/ospool-submit'
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/3f/3fa4b5c6d7e8')
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
        }
        
        when:
        def result = executor.resolveLogFilePath(task)
        
        then:
        result.toString() == '/home/user/.nextflow/ospool-submit/3f/3fa4b5c6d7e8/.condor.log'
    }

    def 'should resolve log file path to work directory when no submitFileDir'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> null
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/7e/7e8f9a0b1c2d')
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
        }
        
        when:
        def result = executor.resolveLogFilePath(task)
        
        then:
        result.toString() == '/staging/work/7e/7e8f9a0b1c2d/.condor.log'
    }

    def 'should use parent directory name as prefix for submit file'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/submit'
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        // Work directory structure: /work/ab/abcdef123456/
        def workDir = Paths.get('/work/ab/abcdef123456')
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
        }
        
        when:
        def result = executor.resolveSubmitFilePath(task)
        
        then:
        // Should use 'ab' as prefix (from parent directory name)
        result.toString() == '/submit/ab/abcdef123456/.command.condor'
        result.parent.name == 'abcdef123456'  // Hash directory
        result.parent.parent.name == 'ab'     // Prefix directory
    }

    def 'should return false for isSharedFilesystem by default'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
        }
        executor.config = config
        executor.name = 'ospool'
        
        expect:
        !executor.isSharedFilesystem()
    }

    def 'should return true for isSharedFilesystem when configured'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> true
        }
        executor.config = config
        executor.name = 'ospool'
        
        expect:
        executor.isSharedFilesystem()
    }

    def 'should detect accessible path from compute nodes'() {
        given:
        def executor = new OspoolExecutor()
        
        expect:
        executor.isAccessibleFromComputeNodes(Paths.get('/staging/work/test'))
        executor.isAccessibleFromComputeNodes(Paths.get('/cvmfs/repo/data'))
        executor.isAccessibleFromComputeNodes(Paths.get('/mnt/gluster/share'))
    }

    def 'should detect inaccessible path from compute nodes'() {
        given:
        def executor = new OspoolExecutor()
        
        expect:
        !executor.isAccessibleFromComputeNodes(Paths.get('/home/user/project'))
        !executor.isAccessibleFromComputeNodes(Paths.get('/tmp/data'))
        !executor.isAccessibleFromComputeNodes(Paths.get('/var/lib/test'))
    }

    def 'should normalize path with auto-staged directories'() {
        given:
        def executor = new OspoolExecutor()
        executor.@autoStagedDirectories = [
            '/home/user/project': Paths.get('/staging/work/.staged-project')
        ]
        
        when:
        def result = executor.normalizePathWithStaging('/home/user/project/script.sh', null)
        
        then:
        result == '/staging/work/.staged-project/script.sh'
    }

    def 'should normalize path with path mappings'() {
        given:
        def executor = new OspoolExecutor()
        def pathMappings = [
            '/mnt/htc-cephfs/fuse/root/staging': '/staging'
        ]
        
        when:
        def result = executor.normalizePathWithStaging('/mnt/htc-cephfs/fuse/root/staging/work/file.txt', pathMappings)
        
        then:
        result == '/staging/work/file.txt'
    }

    def 'should apply auto-staged directories before path mappings'() {
        given:
        def executor = new OspoolExecutor()
        executor.@autoStagedDirectories = [
            '/home/user/project': Paths.get('/staging/work/.staged-project')
        ]
        def pathMappings = [
            '/mnt/htc-cephfs/fuse/root/staging': '/staging'
        ]
        
        when:
        def result = executor.normalizePathWithStaging('/home/user/project/data.txt', pathMappings)
        
        then:
        result == '/staging/work/.staged-project/data.txt'
    }

    def 'should handle longest prefix match for path normalization'() {
        given:
        def executor = new OspoolExecutor()
        def pathMappings = [
            '/mnt/htc-cephfs/fuse/root/staging': '/staging',
            '/mnt/htc-cephfs': '/mnt'
        ]
        
        when:
        def result = executor.normalizePathWithStaging('/mnt/htc-cephfs/fuse/root/staging/work/file.txt', pathMappings)
        
        then:
        // Should match the longer prefix first
        result == '/staging/work/file.txt'
    }

    def 'should return path unchanged when no mappings apply'() {
        given:
        def executor = new OspoolExecutor()
        def pathMappings = [
            '/mnt/htc-cephfs': '/staging'
        ]
        
        when:
        def result = executor.normalizePathWithStaging('/home/user/file.txt', pathMappings)
        
        then:
        result == '/home/user/file.txt'
    }

    def 'should resolve directory path with projectDir variable'() {
        given:
        def executor = new OspoolExecutor()
        def session = Mock(Session) {
            getBaseDir() >> Paths.get('/home/user/myproject')
        }
        executor.session = session
        
        when:
        def result = executor.resolveDirectoryPath('${projectDir}/bin')
        
        then:
        result.toString() == '/home/user/myproject/bin'
    }

    def 'should resolve directory path with baseDir variable'() {
        given:
        def executor = new OspoolExecutor()
        def session = Mock(Session) {
            getBaseDir() >> Paths.get('/home/user/pipeline')
        }
        executor.session = session
        
        when:
        def result = executor.resolveDirectoryPath('${baseDir}/templates')
        
        then:
        result.toString() == '/home/user/pipeline/templates'
    }

    def 'should resolve directory path as literal'() {
        given:
        def executor = new OspoolExecutor()
        def session = Mock(Session)
        executor.session = session
        
        when:
        def result = executor.resolveDirectoryPath('/absolute/path/to/dir')
        
        then:
        result.toString() == '/absolute/path/to/dir'
    }

    def 'should return Path objects unchanged'() {
        given:
        def executor = new OspoolExecutor()
        def session = Mock(Session)
        executor.session = session
        def inputPath = Paths.get('/some/path')
        
        when:
        def result = executor.resolveDirectoryPath(inputPath)
        
        then:
        result.is(inputPath)
    }

    def 'should parse job id from condor_submit output'() {
        given:
        def executor = new OspoolExecutor()
        
        when:
        def result = executor.parseJobId('12345.0 - 1')
        
        then:
        result == '12345.0'
    }

    def 'should return condor_rm as kill command'() {
        given:
        def executor = new OspoolExecutor()
        
        when:
        def result = executor.getKillCommand()
        
        then:
        result == ['condor_rm']
    }

    def 'should return condor_q as queue status command'() {
        given:
        def executor = new OspoolExecutor()
        
        when:
        def result = executor.queueStatusCommand(null)
        
        then:
        result == ['condor_q', '-nobatch']
    }
}
