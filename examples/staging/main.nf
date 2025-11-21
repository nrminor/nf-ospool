#!/usr/bin/env nextflow

/*
 * Example demonstrating directory staging with OSPool executor
 * 
 * This shows how the plugin automatically stages directories
 * that are not accessible from compute nodes.
 */

process processWithScripts {
    debug true
    
    input:
    path script
    val input_data
    
    output:
    stdout
    
    script:
    """
    bash ${script} ${input_data}
    """
}

workflow {
    // Reference a script from your project directory
    // The plugin will automatically stage it to /staging if needed
    script_ch = Channel.fromPath("${projectDir}/scripts/analyze.sh")
    data_ch = Channel.of('data1', 'data2', 'data3')
    
    processWithScripts(script_ch.first(), data_ch)
        | view
}
