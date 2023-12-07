package com.example.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.ClientResponse;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

public class JerseyRequest {
  private ObjectMapper mapper = new ObjectMapper();
  private Client client = Client.create();
  public JsonNode create(String url, Map<String, Object> body) throws IOException {
    WebResource webResource = client.resource(url);
    ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
      .post(ClientResponse.class, mapper.writeValueAsString(body));
    String outputString = response.getEntity(String.class);
    JsonNode jsonNode = mapper.readValue(outputString, JsonNode.class);
    return jsonNode;
  }

  public JsonNode findById(String url) throws JsonMappingException, JsonProcessingException {
    WebResource webResource = client.resource(url);
    ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
      .get(ClientResponse.class);
    String outputString = response.getEntity(String.class);
    JsonNode jsonNode = mapper.readValue(outputString, JsonNode.class);
    return jsonNode;
  }
}
