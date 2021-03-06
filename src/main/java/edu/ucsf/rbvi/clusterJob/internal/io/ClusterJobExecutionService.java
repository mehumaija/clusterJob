package edu.ucsf.rbvi.clusterJob.internal.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.cytoscape.application.CyUserLog;
import org.cytoscape.jobs.CyJob;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.CyJobStatus.Status;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobMonitor;
import org.cytoscape.jobs.CyJobManager;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.session.CySession;
import org.cytoscape.work.TaskMonitor;

import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJob;
import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJobDataService;
import edu.ucsf.rbvi.clusterJob.internal.handlers.ClusterJobHandler;

/**
 * The main interface to the RBVI network cluster REST service.  The
 * general URL format for the execution is:
 * 	http://www.rbvi.ucsf.edu/clusterService/submit?algorithm=alg&param1=param&param2=param&...
 * followed by a JSON data set with all of the edges:
 * {
 * 	edges: [
 * 		{ source: SUID, target: SUID, sourceName: name, targetName: name, attrName: value }
 * 	]
 * }
 * where attrName is the name of the attribute to be used for the cluster algorithm.  
 * This should immediately return with a job id.
 *
 * To check the status of a job:
 * 	http://www.rbvi.ucsf.edu/clusterService/check?jobId=jobId
 *
 * To cancel a job:
 * 	http://www.rbvi.ucsf.edu/clusterService/cancel?jobId=jobId
 *
 * To get the completed data:
 * 	http://www.rbvi.ucsf.edu/clusterService/fetch?jobId=jobId
 *
 * The return
 * value is a very simple JSON:
 * {
 * 	nodes: [
 * 		{ node: SUID, clusterNumber: value }
 * 	]
 * }
 */

public class ClusterJobExecutionService implements CyJobExecutionService {
	static final Logger logger = Logger.getLogger(CyUserLog.NAME);
	static final String COMMAND = "command";
	static final String ERROR = "errorMessage";
	static final String JOBID = "job_id";
	static final String STATUS = "jobStatus";
	static final String STATUS_MESSAGE = "message";
	static final String SUBMIT = "submit";
	final ClusterJobDataService dataService;
	final CyJobManager cyJobManager; //responsible for managing all the running ClusterJobs: polls the changes in the Status, calls some methods in this class
	final CyServiceRegistrar cyServiceRegistrar;

	public enum Command {
		CANCEL("cancel"),
		SUBMIT("submit"),
		FETCH("fetch"),
		CHECK("check");

		String text;
		Command(String text) {
			this.text = text;
		}
		public String toString() { return text; }
	}

	//constructor
	public ClusterJobExecutionService(CyJobManager manager, CyServiceRegistrar registrar) {
		cyJobManager = manager;
		cyServiceRegistrar = registrar;
		dataService = new ClusterJobDataService(cyServiceRegistrar);
	}

	@Override
	public CyJobDataService getDataService() { return dataService; }

	//create a CLusterjob
	@Override
	public CyJob createCyJob(String name) {
		return new ClusterJob(name, null, this, dataService, null, null);
	}

	@Override
	public String getServiceName() { return "ClusterJobExecutionService"; }

	//checks whether the CyJob is a ClusterJob and cancels it, returns the status of the job
	@Override 
	public CyJobStatus cancelJob(CyJob job) {
		System.out.println("Canceling the job!");
		if (job instanceof ClusterJob) {
			JSONObject obj = handleCommand((ClusterJob)job, Command.CANCEL, null);
			return getStatus(obj, null);
		}
		return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob");
	}

	//returns the status of the CLusterJob
	@Override
	public CyJobStatus checkJobStatus(CyJob job) {
		if (job instanceof ClusterJob) {
			JSONObject result = handleCommand((ClusterJob)job, Command.CHECK, null);
			return getStatus(result, null);
		}
		return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob");
	}

	//checks if the CyJob is a ClusterJob, fetches the serialized data (JSON) using DataService methods, puts JSON information in the ClusterJob and returns the status
	@Override
	public CyJobStatus executeJob(CyJob job, String basePath, Map<String, Object> configuration, //configuration comes from network data
	                              CyJobData inputData) {
		
		if (!(job instanceof ClusterJob))
			return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob"); //error message if not clusterjob

		ClusterJob clJob = (ClusterJob)job; //converts CyJob into ClusterJob
		Map<String, String> queryMap = convertConfiguration(configuration); //converts configuration into Map<String, String>

		String serializedData = dataService.getSerializedData(inputData); //gets serialized data (JSON) using dataService
		queryMap.put("inputData", serializedData.toString()); //...and puts it into queryMap as key: "inputData", value: String of the data
		queryMap.put(COMMAND, Command.SUBMIT.toString()); //puts key: COMMAND, value: SUBMIT in the queryMap --> queryMap has two keys
		
		JSONParser parser = new JSONParser();
		JSONObject jsonData = null;
		try {
			jsonData = (JSONObject) parser.parse(serializedData);
		} catch (ParseException e1) {
			System.out.println("Data to JSONObject conversion failed: " + e1.getMessage());
		}
		System.out.println("JSON Data: " + jsonData);
		
		RemoteServer rs = new RemoteServer();
		Object value = null;
		try {
			value = rs.postFile(rs.getServiceURI("leiden"), jsonData);
		} catch (Exception e) {
			System.out.println("Error in postFile method: " + e.getMessage());
		}
		System.out.println("JSON Job ID: " + value);
		
		if (value == null) 
			return new CyJobStatus(Status.ERROR, "Job submission failed!");
		JSONObject json = (JSONObject) value;
		if (!json.containsKey(JOBID)) {
			System.out.println("JSON returned: "+json.toString());
			return new CyJobStatus(Status.ERROR, "Server didn't return an ID!");
		}

		String jobId = json.get(JOBID).toString(); //gets the job ID from the JSON Object
		clJob.setJobId(jobId); //...and sets it to the ClusterJob 
		System.out.println("ClusterJob jobID: " + clJob.getJobId());
		//everything above this is to get the job ID from the JSON jobID repsonse from postFile() and put it in the ClusterJob object
		
		clJob.setBasePath(basePath); //...and also sets the basePath to the Cluster Job
		System.out.println("ClusterJob BasePath: " + clJob.getBasePath());
		
		//getting status
		CyJobStatus jobStatus = checkJobStatus(clJob);

		return jobStatus;
	}

	//fetches JSON object, deserializes the data and puts it to CyJobData
	@Override
	public CyJobStatus fetchResults(CyJob job, CyJobData data) {
		if (job instanceof ClusterJob) {
			JSONObject result = handleCommand((ClusterJob)job, Command.FETCH, null); //handles command FETCH --> argMap is null --> JSON object runs the command

			// Get the unserialized data, dataService deserializes the data (the JSON object), CyJobData is basically a HashMap
			CyJobData newData = dataService.deserialize(result);

			// Merge it in, move the information from newData to data
			for (String key: newData.keySet()) {
				data.put(key, newData.get(key));
			}
			CyJobStatus resultStatus = getStatus(result, null);
			if (resultStatus == null)
				return new CyJobStatus(Status.FINISHED, "Data fetched"); //returns status FINISHED if succesfull
		}
		return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob"); //if not a clusterjob
	}

	@Override
	public CyJob restoreJobFromSession(CySession session, File sessionFile) {
		CyJob job = null;
		try {
			FileReader reader = new FileReader(sessionFile);
			CyJobData sessionData = dataService.deserialize(reader);
			job = getCyJob(sessionData.get("name").toString(), 
			               sessionData.get("path").toString(),
										 sessionData.get("JobId").toString());
			job.setPollInterval((Integer)sessionData.get("pollInterval"));
			String handlerClass = sessionData.get("jobMonitor").toString();
			if (!handlerClass.equals(ClusterJobHandler.class.getCanonicalName())) {
				cyJobManager.associateMonitor(job, handlerClass, -1);
			}
		} catch (FileNotFoundException fnf) {
			logger.error("Unable to read session file!");
		}

		return job;
	}

	//getter, creates new ClusterJob, puts this as the ExecutionService, the same DataService as in this, returns the new clusterjob
	private CyJob getCyJob(String name, String basePath, String jobId) {
		return new ClusterJob(name, basePath, this, dataService, null, jobId);
	}

	@Override
	public void saveJobInSession(CyJob job, File sessionFile) {
		// Create a JSON object for our ClusterJob
		CyJobData sessionData = dataService.getDataInstance();
		sessionData.put("name", job.getJobName());
		sessionData.put("JobId", job.getJobId());
		sessionData.put("path", job.getPath());
		sessionData.put("pollInterval", job.getPollInterval());
		sessionData.put("jobMonitor", job.getJobMonitor().getClass().getCanonicalName());
		String data = dataService.getSerializedData(sessionData).toString();
		try {
			FileWriter writer = new FileWriter(sessionFile);
			writer.write(data);
			writer.close();
		} catch (IOException ioe) {
			logger.error("Unable to save job "+job.getJobId()+" in session!");
		}
	}

	//compare f ex "done" and map that to the status ENUM
	//added return new CyJobStatus
	private CyJobStatus getStatus(JSONObject obj, String message) {
		if (obj.containsKey(STATUS)) {
			Status status = Status.UNKNOWN;
			if (obj.get(STATUS).equals("done")) {
				status = Status.FINISHED;
			} else if (obj.get(STATUS).equals("running")) {
				status = Status.RUNNING;
			}
			// Did we get any information about our status?
			if (obj.containsKey(STATUS_MESSAGE)) {
				if (message == null || message.length() == 0)
					message = (String)obj.get(STATUS_MESSAGE);
				return new CyJobStatus(status, message);
			}
			return new CyJobStatus(status, message);
		} else if (obj.containsKey(ERROR)) {
			return new CyJobStatus(Status.ERROR, (String)obj.get(ERROR));
		}
		return null;
	}

	private JSONObject handleCommand(ClusterJob job, Command command, Map<String, String> argMap) { //argMap contains COMMAND and a JOB ID
		if (argMap == null)
			argMap = new HashMap<>();

		argMap.put(COMMAND, command.toString());
		argMap.put(JOBID, job.getJobId());
		
		RemoteServer rs = new RemoteServer();
		
		JSONObject statusResponse = null;
		try {
			statusResponse = rs.fetchJSON(job.getBasePath() + "status/" + job.getJobId());
		} catch (Exception e) {
			System.out.println("Exception in fetchJSON: " + e.getMessage());
		}
		System.out.println("JSON status response: " + statusResponse);
		
		return statusResponse;
		//return (JSONObject)HttpUtils.postJSON(job.getPath(), argMap, logger); //returns JSONobject, puts in the job path (url), argMap (command and job id) and Logger
	}

	//turns Map<String, Object> into Map<String, String>
	private Map<String, String> convertConfiguration(Map<String, Object> config) {
		Map<String, String> map = new HashMap<>();
		if (config == null || config.size() == 0)
			return map;
		for (String key: config.keySet()) {
			map.put(key, config.get(key).toString());
		}
		return map;
	}
}

