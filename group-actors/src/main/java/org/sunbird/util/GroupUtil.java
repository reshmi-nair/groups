package org.sunbird.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sunbird.models.GroupResponse;
import org.sunbird.models.Member;

public class GroupUtil {

  /**
   * Update Role details in the group of a user
   *
   * @param groups
   * @param members
   * @param userId
   */
  public static void updateRoles(List<GroupResponse> groups, List<Member> members, String userId) {
    if (!groups.isEmpty()) {
      Map<String, String> groupRoleMap = getGroupRoleOfUser(members, userId);
      for (GroupResponse group : groups) {
        group.setMemberRole(groupRoleMap.get(group.getId()));
      }
    }
  }

  /**
   * Get the role of a user in each group and return a mapping with group id
   *
   * @param members
   * @param userId
   * @return groupRoleMap
   */
  private static Map<String, String> getGroupRoleOfUser(List<Member> members, String userId) {
    Map<String, String> groupRoleMap = new HashMap<>();
    members.forEach(
        map -> {
          if (userId.equals(map.getUserId())) {
            groupRoleMap.put(map.getGroupId(), map.getRole());
          }
        });
    return groupRoleMap;
  }
}
