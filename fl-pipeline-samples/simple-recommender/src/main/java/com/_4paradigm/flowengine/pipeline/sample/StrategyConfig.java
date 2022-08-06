package com._4paradigm.flowengine.pipeline.sample;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class StrategyConfig {
    private Double recall;
    private Double sort;
    private Double predict;
    private Double reRank;
}
