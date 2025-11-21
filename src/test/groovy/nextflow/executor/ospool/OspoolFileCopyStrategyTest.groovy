package nextflow.executor.ospool

import nextflow.processor.TaskBean
import spock.lang.Specification

import java.nio.file.Paths

/**
 * Tests for OspoolFileCopyStrategy
 */
class OspoolFileCopyStrategyTest extends Specification {

    def 'should stage input file with path normalization'() {
        given:
        def bean = Mock(TaskBean) {
            getWorkDir() >> Paths.get('/work/dir')
            getInputFiles() >> [:]
        }
        def pathMappings = [
            '/mnt/htc-cephfs/fuse/root/staging': '/staging'
        ]
        def executor = Mock(OspoolExecutor) {
            normalizePathWithStaging(_, _) >> { args -> 
                // Simulate path normalization
                def path = args[0]
                def mappings = args[1]
                if (path.contains('/mnt/htc-cephfs/fuse/root/staging')) {
                    return path.replace('/mnt/htc-cephfs/fuse/root/staging', '/staging')
                }
                return path
            }
        }
        def strategy = new OspoolFileCopyStrategy(bean, pathMappings, executor)
        
        when:
        def result = strategy.stageInputFile(
            Paths.get('/mnt/htc-cephfs/fuse/root/staging/data/input.txt'),
            'input.txt'
        )
        
        then:
        result.contains('/staging/data/input.txt')
        result.contains('input.txt')
    }

    def 'should create parent directories for nested target paths'() {
        given:
        def bean = Mock(TaskBean) {
            getWorkDir() >> Paths.get('/work/dir')
            getInputFiles() >> [:]
        }
        def executor = Mock(OspoolExecutor) {
            normalizePathWithStaging(_, _) >> { args -> args[0] }  // No transformation
        }
        def strategy = new OspoolFileCopyStrategy(bean, [:], executor)
        
        when:
        def result = strategy.stageInputFile(
            Paths.get('/data/input.txt'),
            'subdir/nested/input.txt'
        )
        
        then:
        result.startsWith('mkdir -p')
        result.contains('subdir/nested')
    }

    def 'should not create parent directory for flat target path'() {
        given:
        def bean = Mock(TaskBean) {
            getWorkDir() >> Paths.get('/work/dir')
            getInputFiles() >> [:]
        }
        def executor = Mock(OspoolExecutor) {
            normalizePathWithStaging(_, _) >> { args -> args[0] }  // No transformation
        }
        def strategy = new OspoolFileCopyStrategy(bean, [:], executor)
        
        when:
        def result = strategy.stageInputFile(
            Paths.get('/data/input.txt'),
            'input.txt'
        )
        
        then:
        !result.startsWith('mkdir -p')
    }

    def 'should store path mappings and executor references'() {
        given:
        def bean = Mock(TaskBean) {
            getWorkDir() >> Paths.get('/work/dir')
            getInputFiles() >> [:]
        }
        def pathMappings = [
            '/home/user': '/staging/.staged-home'
        ]
        def executor = Mock(OspoolExecutor)
        
        when:
        def strategy = new OspoolFileCopyStrategy(bean, pathMappings, executor)
        
        then:
        strategy.@pathMappings == pathMappings
        strategy.@executor == executor
    }

    def 'should handle empty path mappings in constructor'() {
        given:
        def bean = Mock(TaskBean) {
            getWorkDir() >> Paths.get('/work/dir')
            getInputFiles() >> [:]
        }
        def executor = Mock(OspoolExecutor)
        
        when:
        def strategy = new OspoolFileCopyStrategy(bean, [:], executor)
        
        then:
        strategy.@pathMappings == [:]
        strategy.@executor == executor
    }

    def 'should handle null path mappings in constructor'() {
        given:
        def bean = Mock(TaskBean) {
            getWorkDir() >> Paths.get('/work/dir')
            getInputFiles() >> [:]
        }
        def executor = Mock(OspoolExecutor)
        
        when:
        def strategy = new OspoolFileCopyStrategy(bean, null, executor)
        
        then:
        strategy.@pathMappings == [:]
        strategy.@executor == executor
    }
}
