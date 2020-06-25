package edu.ucsf.rbvi.clusterJob.internal.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.CyJobStatus.Status;
import org.cytoscape.work.TaskMonitor;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.ucsf.rbvi.clusterJob.internal.handlers.ClusterJobHandler;
import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJob;
import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJobData;
import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJobDataService;


public class RemoteServer {

	private String LOCAL_PATH = "http://localhost:8000/";
	private String PROD_PATH = "http://webservices.rbvi.ucsf.edu/rest/api/v1/";
	private String inputFile = "C:\\Users\\maikk\\Documents\\gsoc\\test_input.csv";
	private CyJobExecutionService executionService;
	private CyJobDataService dataService;
	private ClusterJobHandler jobHandler;
	
	public RemoteServer(CyJobExecutionService executionService, CyJobDataService dataService, ClusterJobHandler jobHandler) {
		this.executionService = executionService;
		this.dataService = dataService;
		this.jobHandler = jobHandler;
	}
	
	public RemoteServer() {
	}
		
		//make the choice between different clustering algorithms/servers
	public String getServiceURI(String server, String service) {
			
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
		
	//read the file and create JSONObject
	public JSONObject createJSONObjectFromData(String dataFileName) throws ParseException {
			
		Map<String, List<String>> data = new HashMap<>();
		List<String> nodesList = new ArrayList<>();
		List<String> edgesList = new ArrayList<>();
			
		try (Scanner scanner = new Scanner(new File(dataFileName))) {
			while (scanner.hasNextLine()) {
				String edge = scanner.nextLine();
					
				if (edge.trim().length() != 0) {
					edgesList.add(edge);
				}
					
				String [] nodes = edge.split(",");
				for (int i = 0; i < 1; i++) {
					if (!nodesList.contains(nodes[i])) {
						nodesList.add(nodes[i]);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Exception reading the file containing the edges data:" + e.getMessage());
		}
			
		data.put("nodes", nodesList);
		data.put("edges", edgesList);
		
		JSONParser parser = new JSONParser();
		JSONObject jsonData = (JSONObject) parser.parse(data.toString());
			
		return jsonData;
	}

		//sends the data
		//you should use a JSONParser to parse the reply and return that.
		//the URI is the whole URI of the service
		//returns the status code and JSONObject (JobID)
	public JSONObject postFile(String uri, JSONObject jsonData, TaskMonitor monitor) throws Exception {
		System.out.println("Posting on: "+uri);
		CloseableHttpClient httpClient = HttpClients.createDefault();  //client = browser --> executes in the default browser of my computer?
		
		CloseableHttpResponse response = getPOSTresponse(uri, jsonData, httpClient);
		
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode != 200 && statusCode != 202) {
			monitor.showMessage(TaskMonitor.Level.ERROR, "Got " 
				+ response.getStatusLine().getStatusCode() + " code from server");
			return null;
		}
			
		BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent())); // it's just a single string that contains a JSON representation of the job id.
			
		JSONParser parser = new JSONParser();
		JSONObject jsonJobID = (JSONObject) parser.parse(reader); //parses the string of jobID into JSONObject

		return jsonJobID;

	}
	
	private CloseableHttpResponse getPOSTresponse(String uri, JSONObject jsonData, CloseableHttpClient httpClient) throws Exception {
		HttpPost httpPost = new HttpPost(uri);
		MultipartEntityBuilder builder = MultipartEntityBuilder.create(); //builds the entity from the JSON data, entity= entire request/response w/o status/request line
		builder.addTextBody("data", jsonData.toString());
		HttpEntity entity = builder.build();
		httpPost.setEntity(entity); //posts the entity
		CloseableHttpResponse response = httpClient.execute(httpPost); //is this the jobID by itself or does it have to be called with some method?
		
		return response;
	}
	
	//this returns the results as JSON
	//CyJobExcService has a checkStatus() and gets results: translate to CyJobStatus that is an ENUM
	//replace the handle command with an appropriate command of remote server
	//parse the json
	public JSONObject fetchJSON(String uri, TaskMonitor monitor) throws Exception {
		System.out.println("Fetching JSON from: "+uri);
		
		CloseableHttpClient httpclient = HttpClients.createDefault();  //client = browser --> executes in the default browser of my computer?
		CloseableHttpResponse response = getGETresponse(uri, httpclient);
		
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode != 200 && statusCode != 202) {
			monitor.showMessage(TaskMonitor.Level.ERROR, "Got "+
			                    response.getStatusLine().getStatusCode()+" code from server");
			return null;
		}
		HttpEntity entity = response.getEntity();
		BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
		
		JSONObject json = (JSONObject) new JSONParser().parse(reader);
		return json; //= dictionary, take it and poll from this the status key Map<Key, value> and for key status there is some answer, JSON similar to xml but easier
	}
	
	private CloseableHttpResponse getGETresponse(String uri, CloseableHttpClient httpclient) throws Exception {
		HttpGet httpGet = new HttpGet(uri);
		CloseableHttpResponse response = httpclient.execute(httpGet);
		
		return response; //pull the status and --> text string and map to the ENUM --> return ENUM
	}
	
	//is this the correct way of getting the key: status? does this give the information we need. My idea here is that the method gets
	//the JSONObject from fetchJSON and gets the key "status" that has the value of status and returns it .toString()
	public String getJobStatusString(JSONObject JSONresponse) {
		Object status = JSONresponse.get("status");
		return status.toString();
	}
	
	//this method maps the status String returned by getJobStatusString with CyJobStatus.Status and returns the equivalent status
	//how can I find out what words the JSONObject status uses?
	public CyJobStatus.Status mapStringStatusToCyJobStatus(String statusString) {

		if (statusString.equals("done")) {
			return Status.FINISHED;
		} else if (statusString.equals("fail")) {
			return Status.ERROR;
		}
		
		return Status.UNKNOWN;
	}
	
	private CyJobStatus getStatus(JSONObject obj) {
		if (obj.containsKey("jobStatus")) {
			Status status = Status.valueOf((String)obj.get("jobStatus"));
			String message = "";
			if (obj.containsKey("message")) {
					message = (String)obj.get("message");
				return new CyJobStatus(status, message);
			}
			return new CyJobStatus(status, message);
		} else if (obj.containsKey("errorMessage")) {
			return new CyJobStatus(Status.ERROR, (String)obj.get("errorMessage"));
		}
		return null;
	}
		
	public void submitJob(TaskMonitor monitor) {
		
			//first, make JSONObject of the data file
		JSONObject data = null;
		try {
			data = createJSONObjectFromData(inputFile);
		} catch (Exception e) {
			System.out.println("Error in creating JSONObject from data: " + e.getMessage());
		}
		System.out.println(data);
		
			//choose the service, now leiden
		String serviceURI = getServiceURI(PROD_PATH, "leiden");
			
			//POST (send) the data to the server and get the jobID as the result
		JSONObject jobIDResponse = null;
		try {
			jobIDResponse = postFile(serviceURI, data, monitor);
		} catch (Exception e) {
			System.out.println("Exception in postFile: " + e.getMessage());
		}
		
			//gets the jobID from the JSON. Is this getter necessary? Can I just toString() the whole JSON response?
		String jobID = jobIDResponse.get("jobId").toString();
		System.out.println(jobID);
		
		ClusterJob clusterJob = new ClusterJob("ClusterJob", PROD_PATH, executionService, dataService, jobHandler, jobID);
		
			//GET the response containing the status from the server
		JSONObject statusResponse = null;
		try {
			statusResponse = fetchJSON(serviceURI, monitor);
		} catch (Exception e) { 
			System.out.println("Exception in fetchJSON: " + e.getMessage());
		}
		
		CyJobStatus jobStatus = getStatus(statusResponse);
		System.out.println(jobStatus);

	}
	
	//need to return the jobID and the status, only that is important for now!!
		
}
