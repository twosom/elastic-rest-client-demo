package com.example.elasticdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ElasticRecruitModel {
    private String positionId;
    private String source;
    private String pageId;
    private String pageUrl;
    private String company;
    private String subject;
    @Builder.Default
    private List<String> etc = new ArrayList<>();
    private String finished;

}
