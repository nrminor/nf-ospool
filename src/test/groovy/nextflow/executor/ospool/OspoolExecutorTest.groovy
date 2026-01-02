package nextflow.executor.ospool

import nextflow.Session
import nextflow.executor.AbstractGridExecutor.QueueStatus
import nextflow.executor.ExecutorConfig
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
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

    // =========================================================================
    // HTCondor Directive Generation Tests
    // =========================================================================

    def 'should generate basic HTCondor directives'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/submit'
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
            getExecConfigProp('ospool', 'getenv', _) >> false
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/ab/abc123')
        def taskConfig = Mock(TaskConfig) {
            getCpus() >> 1
            getMemory() >> null
            getDisk() >> null
            getTime() >> null
            getClusterOptions() >> null
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getConfig() >> taskConfig
        }
        
        when:
        def directives = []
        executor.getDirectives(task, directives)
        
        then:
        directives.contains('universe = vanilla')
        directives.any { it.startsWith('executable = ') && it.contains('.command.run') }
        directives.any { it.startsWith('log = ') }
        directives.contains('queue')
        // Should NOT contain getenv when false
        !directives.contains('getenv = true')
    }

    def 'should include resource requests in directives'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/submit'
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
            getExecConfigProp('ospool', 'getenv', _) >> false
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/ab/abc123')
        def taskConfig = Mock(TaskConfig) {
            getCpus() >> 4
            getMemory() >> new MemoryUnit('8 GB')
            getDisk() >> new MemoryUnit('10 GB')
            getTime() >> Duration.of('2h')
            getClusterOptions() >> null
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getConfig() >> taskConfig
        }
        
        when:
        def directives = []
        executor.getDirectives(task, directives)
        
        then:
        directives.contains('request_cpus = 4')
        directives.contains('machine_count = 1')
        directives.any { it.contains('request_memory') && it.contains('8 GB') }
        directives.any { it.contains('request_disk') && it.contains('10 GB') }
        // 2h = 7200 seconds
        directives.any { it.contains('periodic_remove') && it.contains('7200') }
    }

    def 'should include getenv when shared filesystem'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> null
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> true
            getExecConfigProp('ospool', 'getenv', true) >> true
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/ab/abc123')
        def taskConfig = Mock(TaskConfig) {
            getCpus() >> 1
            getMemory() >> null
            getDisk() >> null
            getTime() >> null
            getClusterOptions() >> null
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getConfig() >> taskConfig
        }
        
        when:
        def directives = []
        executor.getDirectives(task, directives)
        
        then:
        directives.contains('getenv = true')
    }

    def 'should include clusterOptions as string in directives'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/submit'
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
            getExecConfigProp('ospool', 'getenv', _) >> false
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/ab/abc123')
        def taskConfig = Mock(TaskConfig) {
            getCpus() >> 1
            getMemory() >> null
            getDisk() >> null
            getTime() >> null
            getClusterOptions() >> 'HasCHTCStaging=true'
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getConfig() >> taskConfig
        }
        
        when:
        def directives = []
        executor.getDirectives(task, directives)
        
        then:
        directives.contains('HasCHTCStaging=true')
    }

    def 'should include clusterOptions as list in directives'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/submit'
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
            getExecConfigProp('ospool', 'getenv', _) >> false
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/ab/abc123')
        def taskConfig = Mock(TaskConfig) {
            getCpus() >> 1
            getMemory() >> null
            getDisk() >> null
            getTime() >> null
            getClusterOptions() >> ['HasCHTCStaging=true', '+WantFlocking=true']
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getConfig() >> taskConfig
        }
        
        when:
        def directives = []
        executor.getDirectives(task, directives)
        
        then:
        directives.contains('HasCHTCStaging=true')
        directives.contains('+WantFlocking=true')
    }

    def 'should split semicolon-separated clusterOptions'() {
        given:
        def executor = new OspoolExecutor()
        def config = Mock(ExecutorConfig) {
            getExecConfigProp('ospool', 'submitFileDir', null) >> '/submit'
            getExecConfigProp('ospool', 'sharedFilesystem', false) >> false
            getExecConfigProp('ospool', 'getenv', _) >> false
        }
        executor.config = config
        executor.name = 'ospool'
        
        and:
        def workDir = Paths.get('/staging/work/ab/abc123')
        def taskConfig = Mock(TaskConfig) {
            getCpus() >> 1
            getMemory() >> null
            getDisk() >> null
            getTime() >> null
            getClusterOptions() >> 'HasCHTCStaging=true; +WantFlocking=true'
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> workDir
            getConfig() >> taskConfig
        }
        
        when:
        def directives = []
        executor.getDirectives(task, directives)
        
        then:
        directives.contains('HasCHTCStaging=true')
        directives.contains('+WantFlocking=true')
    }

    // =========================================================================
    // Queue Status Parsing Tests
    // =========================================================================

    def 'should parse condor_q output'() {
        given:
        def executor = new OspoolExecutor()
        def output = '''\
            
            -- Schedd: submit.chtc.wisc.edu : <128.104.100.43:9618?...>
             ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE  CMD
            1234.0   user            1/2  10:30   0+00:01:23 R  0   0.0  script.sh
            1235.0   user            1/2  10:31   0+00:00:00 I  0   0.0  script.sh
            1236.0   user            1/2  10:32   0+00:05:00 H  0   0.0  script.sh
            1237.0   user            1/2  10:33   0+00:00:00 C  0   0.0  script.sh
            
            4 jobs; 1 completed, 0 removed, 1 idle, 1 running, 1 held, 0 suspended
            '''.stripIndent()
        
        when:
        def result = executor.parseQueueStatus(output)
        
        then:
        result['1234.0'] == QueueStatus.RUNNING
        result['1235.0'] == QueueStatus.PENDING
        result['1236.0'] == QueueStatus.HOLD
        result['1237.0'] == QueueStatus.DONE
    }

    def 'should handle all HTCondor status codes'() {
        given:
        def executor = new OspoolExecutor()
        
        expect:
        OspoolExecutor.DECODE_STATUS['U'] == QueueStatus.PENDING   // Unexpanded
        OspoolExecutor.DECODE_STATUS['I'] == QueueStatus.PENDING   // Idle
        OspoolExecutor.DECODE_STATUS['R'] == QueueStatus.RUNNING   // Running
        OspoolExecutor.DECODE_STATUS['X'] == QueueStatus.ERROR     // Removed
        OspoolExecutor.DECODE_STATUS['C'] == QueueStatus.DONE      // Completed
        OspoolExecutor.DECODE_STATUS['H'] == QueueStatus.HOLD      // Held
        OspoolExecutor.DECODE_STATUS['E'] == QueueStatus.ERROR     // Error
    }

    def 'should handle unknown status codes gracefully'() {
        given:
        def executor = new OspoolExecutor()
        def output = '''\
             ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE  CMD
            1234.0   user            1/2  10:30   0+00:01:23 Z  0   0.0  script.sh
            
            '''.stripIndent()
        
        when:
        def result = executor.parseQueueStatus(output)
        
        then:
        result['1234.0'] == QueueStatus.UNKNOWN
    }

    def 'should handle empty queue output'() {
        given:
        def executor = new OspoolExecutor()
        
        expect:
        executor.parseQueueStatus('').isEmpty()
        executor.parseQueueStatus(null).isEmpty()
    }

    def 'should stop parsing at empty line'() {
        given:
        def executor = new OspoolExecutor()
        def output = '''\
             ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE  CMD
            1234.0   user            1/2  10:30   0+00:01:23 R  0   0.0  script.sh
            
            Total for query: 1 jobs
            '''.stripIndent()
        
        when:
        def result = executor.parseQueueStatus(output)
        
        then:
        result.size() == 1
        result['1234.0'] == QueueStatus.RUNNING
    }

    // =========================================================================
    // Directory Staging Tests
    // =========================================================================

    @TempDir
    Path tempDir

    def 'should stage directory excluding hidden files'() {
        given:
        def executor = new OspoolExecutor()
        def sourceDir = tempDir.resolve('project')
        def workDir = tempDir.resolve('work')
        
        // Create source structure
        Files.createDirectories(sourceDir.resolve('bin'))
        Files.createDirectories(sourceDir.resolve('.hidden'))
        Files.write(sourceDir.resolve('main.nf'), 'workflow {}'.bytes)
        Files.write(sourceDir.resolve('bin/script.sh'), '#!/bin/bash'.bytes)
        Files.write(sourceDir.resolve('.gitignore'), 'work/'.bytes)
        Files.write(sourceDir.resolve('.hidden/secret'), 'hidden'.bytes)
        
        executor.session = Mock(Session) { getWorkDir() >> workDir }
        Files.createDirectories(workDir)
        
        when:
        def staged = executor.stageDirectory(sourceDir)
        
        then:
        staged.toString().contains('.staged-project')
        Files.exists(staged.resolve('main.nf'))
        Files.exists(staged.resolve('bin/script.sh'))
        !Files.exists(staged.resolve('.gitignore'))  // Hidden file excluded
        !Files.exists(staged.resolve('.hidden'))     // Hidden dir excluded
    }

    def 'should stage directory excluding work directories'() {
        given:
        def executor = new OspoolExecutor()
        def sourceDir = tempDir.resolve('project')
        def workDir = tempDir.resolve('staging-work')
        
        // Create source structure with excluded directories
        Files.createDirectories(sourceDir.resolve('src'))
        Files.createDirectories(sourceDir.resolve('work/ab/abc123'))
        Files.createDirectories(sourceDir.resolve('results'))
        Files.createDirectories(sourceDir.resolve('.git/objects'))
        Files.createDirectories(sourceDir.resolve('.nextflow/history'))
        Files.write(sourceDir.resolve('main.nf'), 'workflow {}'.bytes)
        Files.write(sourceDir.resolve('src/lib.groovy'), 'class Lib {}'.bytes)
        Files.write(sourceDir.resolve('work/ab/abc123/.command.sh'), 'echo hi'.bytes)
        Files.write(sourceDir.resolve('.git/config'), '[core]'.bytes)
        
        executor.session = Mock(Session) { getWorkDir() >> workDir }
        Files.createDirectories(workDir)
        
        when:
        def staged = executor.stageDirectory(sourceDir)
        
        then:
        Files.exists(staged.resolve('main.nf'))
        Files.exists(staged.resolve('src/lib.groovy'))
        !Files.exists(staged.resolve('work'))       // work/ excluded
        !Files.exists(staged.resolve('results'))    // results/ excluded
        !Files.exists(staged.resolve('.git'))       // .git/ excluded
        !Files.exists(staged.resolve('.nextflow'))  // .nextflow/ excluded
    }

    def 'should stage secrets directory with only secret files'() {
        given:
        def executor = new OspoolExecutor()
        def secretsDir = tempDir.resolve('.nextflow/secrets')
        def workDir = tempDir.resolve('work')
        
        Files.createDirectories(secretsDir)
        Files.createDirectories(workDir)
        Files.write(secretsDir.resolve('.nf-MY_SECRET.secrets'), 'secret-value'.bytes)
        Files.write(secretsDir.resolve('.nf-OTHER.secrets'), 'other-secret'.bytes)
        Files.write(secretsDir.resolve('other-file.txt'), 'not a secret'.bytes)
        Files.write(secretsDir.resolve('random.secrets'), 'wrong pattern'.bytes)
        
        executor.session = Mock(Session) { getWorkDir() >> workDir }
        
        when:
        def staged = executor.stageDirectory(secretsDir)
        
        then:
        staged.toString().contains('.staged-secrets')
        Files.exists(staged.resolve('.nf-MY_SECRET.secrets'))
        Files.exists(staged.resolve('.nf-OTHER.secrets'))
        !Files.exists(staged.resolve('other-file.txt'))   // Not .nf-*.secrets pattern
        !Files.exists(staged.resolve('random.secrets'))   // Doesn't start with .nf-
    }

    def 'should preserve file contents when staging'() {
        given:
        def executor = new OspoolExecutor()
        def sourceDir = tempDir.resolve('project')
        def workDir = tempDir.resolve('work')
        
        Files.createDirectories(sourceDir)
        Files.createDirectories(workDir)
        def content = 'workflow { println "hello" }'
        Files.write(sourceDir.resolve('main.nf'), content.bytes)
        
        executor.session = Mock(Session) { getWorkDir() >> workDir }
        
        when:
        def staged = executor.stageDirectory(sourceDir)
        
        then:
        new String(Files.readAllBytes(staged.resolve('main.nf'))) == content
    }

    def 'should throw exception when staging fails'() {
        given:
        def executor = new OspoolExecutor()
        def sourceDir = tempDir.resolve('nonexistent')
        def workDir = tempDir.resolve('work')
        
        executor.session = Mock(Session) { getWorkDir() >> workDir }
        Files.createDirectories(workDir)
        
        when:
        executor.stageDirectory(sourceDir)
        
        then:
        def e = thrown(IllegalStateException)
        e.message.contains('Failed to stage directory')
    }
}
