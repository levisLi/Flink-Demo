package com.galaxy.neptune.flink.bean;

import com.beust.jcommander.Parameter;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class CommandArgs {
    /** config file path */
    @Parameter(
            names = {"-f", "--file"},
            description = "Config file")
    protected String configFile;

    @Parameter(
            names = {"-p", "--properties"},
            description = "properties file")
    protected String propertiesFile;

    public String getConfigFile() {
        return configFile;
    }

    public String getPropertiesFile() {
        return propertiesFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }
}
