#!/usr/bin/env nextflow

/*
 * Example using containers with the OSPool executor
 * 
 * This demonstrates how to use Docker/Singularity containers
 * with the OSPool executor.
 */

process analyzeWithContainer {
    container 'ubuntu:22.04'
    debug true
    
    input:
    val sample
    
    output:
    stdout
    
    script:
    """
    echo "Processing sample: ${sample}"
    echo "Running in container:"
    cat /etc/os-release | grep PRETTY_NAME
    uname -a
    """
}

workflow {
    Channel.of('sample1', 'sample2', 'sample3')
        | analyzeWithContainer
        | view
}
