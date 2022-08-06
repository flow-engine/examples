package com._4paradigm.flowengine.pipeline.sample;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"itemId"})
public class Item {

    private String itemId;
    private double recallScore;
    private double algoScore;
    private String channel;
}
