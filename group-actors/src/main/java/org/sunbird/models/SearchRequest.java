package org.sunbird.models;

import java.util.Map;

public class SearchRequest {
  private Map<String, Object> request;

  public Map<String, Object> getRequest() {
    return request;
  }

  public void setRequest(Map<String, Object> request) {
    this.request = request;
  }
}
