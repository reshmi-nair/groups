package org.sunbird.models;

import java.util.List;

public class GroupResponse extends Group {

  private List<MemberResponse> members;

  private String memberRole;

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
