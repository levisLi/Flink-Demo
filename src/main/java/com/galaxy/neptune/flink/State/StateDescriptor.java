package com.galaxy.neptune.flink.State;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

public class StateDescriptor {
    //自定义source状态描述器
    public final static ListStateDescriptor SOURCE_IDS_DESC = new ListStateDescriptor("ids", TypeInformation.of(new TypeHint<List<Long>>() {
    }));

}
