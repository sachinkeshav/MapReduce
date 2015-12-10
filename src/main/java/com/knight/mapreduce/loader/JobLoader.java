package com.knight.mapreduce.loader;

import org.apache.commons.cli.CommandLine;

import com.knight.mapreduce.options.MyOptions;

/**
 * 
 * @author sachinkeshav
 *
 */
public interface JobLoader {

	public int process(CommandLine commandLine, MyOptions options)  throws Exception;
}
