package org.sunbird.models;

import org.apache.commons.lang3.StringUtils;

public class MemberResponse {
  private static final long serialVersionUID = 7528802960267784945L;

  private String userId;
  private String groupId;
  private String role;
  private String status;
  private String createdOn;
  private String createdBy;
  private String updatedOn;
  private String updatedBy;
  private String removedOn;
  private String removedBy;

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    if (StringUtils.isNotBlank(role)) {
      this.role = role;
    }
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    if (StringUtils.isNotBlank(status)) {
      this.status = status;
    }
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

  public String getRemovedOn() {
    return removedOn;
  }

  public void setRemovedOn(String removedOn) {
    this.removedOn = removedOn;
  }

  public String getRemovedBy() {
    return removedBy;
  }

  public void setRemovedBy(String removedBy) {
    this.removedBy = removedBy;
  }

  private String userName = "";

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getUserName() {
    return userName;
  }
}
