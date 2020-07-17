package org.sunbird.models;

/** This enum will contains different operation for group APIs */
public enum ActorOperations {
  CREATE_GROUP("createGroup"),
  UPDATE_GROUP("updateGroup"),
  READ_GROUP("readGroup"),
  SEARCH_GROUP("searchGroup"),
  SET_CACHE("setCache"),
  DEL_CACHE("delCache"),
  GET_CACHE("getCache");

  private String value;

  /**
   * constructor
   *
   * @param value String
   */
  ActorOperations(String value) {
    this.value = value;
  }

  /**
   * returns the enum value
   *
   * @return String
   */
  public String getValue() {
    return this.value;
  }
}
