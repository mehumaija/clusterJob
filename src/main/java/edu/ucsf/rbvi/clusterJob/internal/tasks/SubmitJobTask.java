package edu.ucsf.rbvi.clusterJob.internal.tasks;

import java.util.List;

import org.cytoscape.application.CyApplicationManager;
import org.cytoscape.jobs.CyJob;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobManager;
import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.SUIDUtil;
import org.cytoscape.model.CyEdge;
import org.cytoscape.model.CyIdentifiable;
import org.cytoscape.model.CyNetwork;
import org.cytoscape.model.CyNode;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.task.AbstractNetworkTask;
import org.cytoscape.work.TaskIterator;
import org.cytoscape.work.TaskMonitor;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import edu.ucsf.rbvi.clusterJob.internal.handlers.ClusterJobHandler;
import edu.ucsf.rbvi.clusterJob.internal.io.RemoteServer;

public class SubmitJobTask extends AbstractNetworkTask {
	final CyServiceRegistrar registrar;

	public SubmitJobTask(CyNetwork network, CyServiceRegistrar registrar) {
		super(network); //CyNetwork is the primary interface for representing networks in cytoscape
		this.registrar = registrar;
	}

	public void run(TaskMonitor monitor) throws ParseException {
		System.out.println("Inside SubmitJobTask run method!!");
		// Get the execution service
		CyJobExecutionService executionService = 
						registrar.getService(CyJobExecutionService.class, "(title=ClusterJobExecutor)");
		CyApplicationManager appManager = registrar.getService(CyApplicationManager.class);
		CyNetwork currentNetwork = appManager.getCurrentNetwork(); //gets the network presented in Cytoscape
		
		List<List<String>> edgesList = RemoteServer.createEdgesList();
		List<String> nodesList = RemoteServer.createNodesList();
		String basePath = RemoteServer.getBasePath();
		
		// Get our initial job
		CyJob job = executionService.createCyJob("ClusterJob"); //creates a new ClusterJob object
		// Get the data service
		CyJobDataService dataService = job.getJobDataService(); //gets the dataService of the execution service
		// Add our data
		CyJobData jobData = dataService.addData(null, "nodes", nodesList);
		jobData = dataService.addData(jobData, "edges", edgesList);
//		CyJobData jobData = dataService.addData(null, "network", currentNetwork, currentNetwork.getNodeList(), null, null); //CyJobData data, String key, CyNetwork network, List<? extends CyIdentifiable> nodesAndEdges, List<String> nodeColumns, List<String> edgeColumns
		// Create our handler
		ClusterJobHandler jobHandler = new ClusterJobHandler(job, network);
		job.setJobMonitor(jobHandler);
		
		// Submit the job
		CyJobStatus exStatus = executionService.executeJob(job, basePath, null, jobData);
		if (exStatus.getStatus().equals(CyJobStatus.Status.ERROR) ||
		    exStatus.getStatus().equals(CyJobStatus.Status.UNKNOWN)) {
			monitor.showMessage(TaskMonitor.Level.ERROR, exStatus.toString());
			return;
		}
		System.out.println("Back to SubmitJobTask! ExStatus is : " + exStatus);
		
		// Save our SUIDs in case we get saved and restored
		SUIDUtil.saveSUIDs(job, currentNetwork, currentNetwork.getNodeList());

		CyJobManager manager = registrar.getService(CyJobManager.class);
		manager.addJob(job, jobHandler, 5);
	}
	
	
}

