#!/usr/bin/env nextflow

/*
 * Basic example using the OSPool executor
 * 
 * This demonstrates the minimal configuration needed to run
 * a simple Nextflow pipeline on the Open Science Pool.
 */

process sayHello {
    debug true
    
    input:
    val greeting
    
    output:
    stdout
    
    script:
    """
    echo "$greeting from OSPool!"
    hostname
    """
}

workflow {
    Channel.of('Hello', 'Hola', 'Bonjour', 'Ciao') 
        | sayHello 
        | view
}
