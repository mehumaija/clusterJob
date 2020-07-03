package edu.ucsf.rbvi.clusterJob.internal.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import edu.ucsf.rbvi.clusterJob.internal.io.ClusterJobExecutionService.Command;


public class RemoteServer {

	static private String LOCAL_PATH = "http://localhost:8000/";
	static private String PROD_PATH = "http://webservices.rbvi.ucsf.edu/rest/api/v1/";
	static private String inputFile = "C:\\Users\\maikk\\Documents\\gsoc\\test_input.csv";
	
		
	static public String getBasePath() {
		return PROD_PATH;
	}
	
		//make the choice between different clustering algorithms/servers, this works
	static public String getServiceURI(String service) {
		
		String server = PROD_PATH;
		
		if (service.equals("leiden")) {
			return server + "service/leiden?objective_function=modularity&iterations=4";
		} else if (service.equals("fastgreedy")) {
			return server + "service/fastgreedy";
		} else if (service.equals("infomap")) {
			return server + "service/infomap";
		} else if (service.equals("labelpropagation")) {
			return server + "service/labelpropagation";
		} else if (service.equals("leadingeigenvector")) {
			return server + "service/leadingeigenvector";
		} else if (service.equals("multilevel")) {
			return server + "service/multilevel";
		} else {
			System.out.println("Unknown service");
		}
			
		return null;
	}
		
	static public List<String> createNodesList() {
		List<String> nodesList = new ArrayList<>();

		try (Scanner scanner = new Scanner(new File(inputFile))) {

			while (scanner.hasNextLine()) {
				String edge = scanner.nextLine();
				
				String [] nodesAndWeights = edge.split(",");
				
				for (int i = 0; i < 2; i++) {
					if (!nodesList.contains(nodesAndWeights[i])) {
						nodesList.add(nodesAndWeights[i]);
					}
				}
			}
			
		} catch (Exception e) {
			System.out.println("Exception reading the file containing the edges data:" + e.getMessage());
		}
		
		return nodesList;
	}
	
	static public List<List<String>> createEdgesList() {
		
		List<List<String>> edgesList = new ArrayList<>();
		
		try (Scanner scanner = new Scanner(new File(inputFile))) {
			
			while (scanner.hasNextLine()) {
				String edge = scanner.nextLine();
				
				String [] nodesAndWeights = edge.split(",");
				ArrayList<String> edgeArray = new ArrayList<>();
				for (int i = 0; i < 3; i++) {
					edgeArray.add(nodesAndWeights[i]);
				}
				edgesList.add(edgeArray);
			}
		} catch(Exception e) {
			System.out.println("Exception reading the file containing the edges data:" + e.getMessage());
		}
		
		return edgesList;
		
	}

		//sends the data
		//you should use a JSONParser to parse the reply and return that.
		//the URI is the whole URI of the service
		//returns the status code and JSONObject (JobID)
	static public JSONObject postFile(String uri, JSONObject jsonData) throws Exception {
		System.out.println("Posting on: " + uri);
		CloseableHttpClient httpClient = HttpClients.createDefault();  //client = browser --> executes in the default browser of my computer?
		
		CloseableHttpResponse response = getPOSTresponse(uri, jsonData, httpClient);
		System.out.println("HttpPOST response: " + response.toString());
		
		int statusCode = response.getStatusLine().getStatusCode();
		System.out.println("HttpPOST status: " + statusCode);
		if (statusCode != 200 && statusCode != 202) {
			return null;
		}
			
		BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent())); // it's just a single string that contains a JSON representation of the job id.
		
		JSONParser parser = new JSONParser();
		JSONObject jsonJobID = (JSONObject) parser.parse(reader); //parses the string of jobID into JSONObject
		
		
		return jsonJobID;
	}
	
	static private CloseableHttpResponse getPOSTresponse(String uri, JSONObject jsonData, CloseableHttpClient httpClient) throws Exception {
		HttpPost httpPost = new HttpPost(uri);
		MultipartEntityBuilder builder = MultipartEntityBuilder.create(); //builds the entity from the JSON data, entity= entire request/response w/o status/request line
		builder.addTextBody("data", jsonData.toString());
		HttpEntity entity = builder.build();
		System.out.println("entity: " + entity.toString());
		httpPost.setEntity(entity); //posts the entity
		System.out.println("httpPost: " + httpPost.toString());
		CloseableHttpResponse response = httpClient.execute(httpPost); //is this the jobID by itself or does it have to be called with some method?
		
		return response;
	}
	
	//this returns the results as JSON
	//CyJobExcService has a checkStatus() and gets results: translate to CyJobStatus that is an ENUM
	//replace the handle command with an appropriate command of remote server
	//parse the json
	static public JSONObject fetchJSON(String uri, Command command) throws Exception {
		System.out.println("Fetching JSON from: " + uri);
		
		CloseableHttpClient httpclient = HttpClients.createDefault();  //client = browser --> executes in the default browser of my computer?
		HttpGet httpGet = new HttpGet(uri);
		CloseableHttpResponse response = httpclient.execute(httpGet);
		
		System.out.println("HttpGET response: " + response.toString());
		
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode != 200 && statusCode != 202) {
			return null;
		}
		HttpEntity entity = response.getEntity();
		BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
		
		JSONObject json= new JSONObject();
		if (command == Command.CHECK) {
			String line = "";
			Object message = null;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
				json.put("jobStatus", line);
				if (json.containsKey("message")) {
					message = json.get("message");
					json.put("message: ", message);
				} else {
					json.put("message", line);
				}
			}
		} else if (command == Command.FETCH) {
			JSONParser parser = new JSONParser();
			json = (JSONObject) parser.parse(reader); 
		}
		
	
	   
		return json; //= dictionary, take it and poll from this the status key Map<Key, value> and for key status there is some answer, JSON similar to xml but easier
	}



}
