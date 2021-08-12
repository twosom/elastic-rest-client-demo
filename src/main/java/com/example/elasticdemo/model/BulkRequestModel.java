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
public class BulkRequestModel {

    private String positionId;
    private String recruitUrl;
    private String source;
    private String subject;
    private String companyName;
    private String companyAddress;
    private List<String> description = new ArrayList<>();
    private String position;
    private String finished;
    private String workArea;
    private List<String> employmentType = new ArrayList<>();
    private List<String> companyInformation = new ArrayList<>();
    private List<String> welfare = new ArrayList<>();
    private String categoryBusiness;
    private List<String> hashTag = new ArrayList<>();
    private String externalRecruitUrl;
    private String created;


}

