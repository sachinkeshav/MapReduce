package com.knight.mapreduce;

import org.apache.commons.cli.*;

/**
 * 
 * @author sachinkeshav
 *
 */
public class MyOptions {

	public Option help;
    public Option dot;
    public Option dotFile;
    public Option input;
    public Option output;
    public Option job;
    public Option jobConfig;
    public Option idpConfig;
    public Option clientId;
    public Option configFile;
    public Option clientConfigFile;
    public Option index;
    public Option layout; // Used to override default layout. If not specified the layout is used based on the fileName
    public Option recordType; // Used to override the default . If not specified the record type is based on the layoutFileName
    public Option outputLayout;
    public Option clusterConfig;
    protected Options options;

    protected MyOptions() {
    }

    public static MyOptions createOptions() {
        MyOptions myOptions = new MyOptions();
        myOptions.initializeOptions();
        return (myOptions);
    }

	public CommandLine parse(String[] args) {
        try {
            GnuParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(options, args);
            return (commandLine);
        } catch (ParseException pe) {
            System.out.println(pe);
            return (null);
        }
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar MapReduce.jar", options);
    }

    protected void initializeOptions() {
        help        = addOption("help", "Help");
        dot         = addOption("dot", "Creates default dot file");
        dotFile     = addOptionWithArg("dotFile", "Creates user dot file");
        input       = addOptionWithArg("input", "Input File, a list of locations ; separated if required multiple inputs from job. For example utilization metric loader");
        output      = addOptionWithArg("output", "Output File");
        job         = addOptionWithArg("job", "Job to run");
        jobConfig   = addOptionWithArg("jobConfig", "Job Config file");
        idpConfig   = addOptionWithArg("idpConf", "Config file for IDP");
        clientId    = addOptionWithArg("clientId", "Client Id of the dataset to process");
        configFile  = addOptionWithArg("configFile", "Configuration file location for additional configuration required");
        clientConfigFile  = addOptionWithArg("clientConfigFile", "Configuration file location for client specific  configuration required");
        index       = addOptionWithArg("index","Index of Elasticsearch to load data into");
        layout      = addOptionWithArg("layout", "The layout to be used.If not specified the layout is based on the filename");
        recordType  = addOptionWithArg("recordType", "The record type of elastic search. If not specified the recordType is based on the layout");
        outputLayout=addOptionWithArg("outputLayout","List of comma separated output layout files to be used for sink layout");
        clusterConfig=addOptionWithArg("clusterConfig","Configuration file location for elasticsearch cluster");
        options = new Options();
        options.addOption(help);
        options.addOption(dot);
        options.addOption(dotFile);
        options.addOption(input);
        options.addOption(output);
        options.addOption(idpConfig);
        options.addOption(job);
        options.addOption(jobConfig);
        options.addOption(clientId);
        options.addOption(configFile);
        options.addOption(clientConfigFile);
        options.addOption(index);
        options.addOption(layout);
        options.addOption(recordType);
        options.addOption(outputLayout);
        options.addOption(clusterConfig);
    }

	private Option addOption(String option, String description) {

        OptionBuilder.withArgName(option);
        OptionBuilder.withDescription(description);
        Option argOption = OptionBuilder.create(option);
        return (argOption);
    }

	private Option addOptionWithArg(String option, String description) {

        OptionBuilder.withArgName(option);
        OptionBuilder.withDescription(description);
        OptionBuilder.hasArg();
        Option argOption = OptionBuilder.create(option);
        return (argOption);
    }
}
