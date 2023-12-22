package com.galaxy.neptune.flink.State;

import org.apache.flink.util.OutputTag;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class OutPutTag {

    public static final OutputTag<String> APPLY_OUTPUT_TAG = new OutputTag<String>("applyOutputTag"){};

    public static final OutputTag<String> REPORT_OUTPUT_TAG = new OutputTag<String>("reportOutputTag"){};

    public static final OutputTag<String> STUDY_OUTPUT_TAG = new OutputTag<String>("studyOutputTag"){};

    public static final OutputTag<String> SERIES_OUTPUT_TAG = new OutputTag<String>("seriesOutputTag"){};

}
