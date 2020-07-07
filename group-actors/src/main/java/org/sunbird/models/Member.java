package org.sunbird.models;

import java.sql.Timestamp;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.util.JsonKey;

public class Member {

  private String userId;
  private String role = JsonKey.MEMBER;
  private String groupId;
  private String status = JsonKey.ACTIVE;
  private Timestamp createdOn;
  private String createdBy;
  private Timestamp updatedOn;
  private String updatedBy;
  private Timestamp removedOn;
  private String removedBy;

  public Member() {}

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

  public Timestamp getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(Timestamp createdOn) {
    this.createdOn = createdOn;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public Timestamp getUpdatedOn() {
    return updatedOn;
  }

  public void setUpdatedOn(Timestamp updatedOn) {
    this.updatedOn = updatedOn;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public void setUpdatedBy(String updatedBy) {
    this.updatedBy = updatedBy;
  }

  public Timestamp getRemovedOn() {
    return removedOn;
  }

  public void setRemovedOn(Timestamp removedOn) {
    this.removedOn = removedOn;
  }

  public String getRemovedBy() {
    return removedBy;
  }

  public void setRemovedBy(String removedBy) {
    this.removedBy = removedBy;
  }
}
