package org.sunbird.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GroupResponse {

  private static final long serialVersionUID = 7529802960267784945L;

  private String id;
  private String name;
  private String description;
  private String status;
  private String membershipType;
  private List<Map<String, Object>> activities;
  private String createdOn;
  private String createdBy;
  private String updatedOn;
  private String updatedBy;
  private List<MemberResponse> members;
  private String memberRole;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getMembershipType() {
    return membershipType;
  }

  public void setMembershipType(String membershipType) {
    this.membershipType = membershipType;
  }

  public List<Map<String, Object>> getActivities() {
    return activities;
  }

  public void setActivities(List<Map<String, Object>> activities) {
    this.activities = activities;
  }

  public String getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getUpdatedOn() {
    return updatedOn;
  }

  public void setUpdatedOn(String updatedOn) {
    this.updatedOn = updatedOn;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public void setUpdatedBy(String updatedBy) {
    this.updatedBy = updatedBy;
  }

  public void setMemberRole(String role) {
    this.memberRole = role;
  }

  public String getMemberRole() {
    return memberRole;
  }

  public List<MemberResponse> getMembers() {
    return members;
  }

  public void setMembers(List<MemberResponse> members) {
    this.members = members;
  }
}
