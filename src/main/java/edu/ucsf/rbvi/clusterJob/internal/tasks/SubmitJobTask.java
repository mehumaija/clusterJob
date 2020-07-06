package edu.ucsf.rbvi.clusterJob.internal.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cytoscape.application.CyApplicationManager;
import org.cytoscape.jobs.CyJob;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobManager;
import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.SUIDUtil;
import org.cytoscape.model.CyColumn;
import org.cytoscape.model.CyEdge;
import org.cytoscape.model.CyIdentifiable;
import org.cytoscape.model.CyNetwork;
import org.cytoscape.model.CyNode;
import org.cytoscape.model.CyRow;
import org.cytoscape.model.CyTable;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.task.AbstractNetworkTask;
import org.cytoscape.work.TaskIterator;
import org.cytoscape.work.TaskMonitor;
import org.json.simple.JSONArray;
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
		// Get the execution service
		CyJobExecutionService executionService = 
						registrar.getService(CyJobExecutionService.class, "(title=ClusterJobExecutor)");
		CyApplicationManager appManager = registrar.getService(CyApplicationManager.class);
		CyNetwork currentNetwork = appManager.getCurrentNetwork(); //gets the network presented in Cytoscape
		System.out.println("Current network: " + currentNetwork.toString());
		
		HashMap<Long, String> nodeMap = getNetworkNodes(currentNetwork);
		List<String> nodeArray = new ArrayList<>();
		for (Long nodeSUID : nodeMap.keySet()) {
			nodeArray.add(nodeMap.get(nodeSUID));
		}
		
		System.out.println("Node array from the current network: " + nodeArray);
		
		List<List<String>> edgeArray = getNetworkEdges(currentNetwork, nodeMap);
		System.out.println("Edges from the current network: " + edgeArray);
		
		String basePath = RemoteServer.getBasePath();
		
		// Get our initial job
		CyJob job = executionService.createCyJob("ClusterJob"); //creates a new ClusterJob object
		// Get the data service
		CyJobDataService dataService = job.getJobDataService(); //gets the dataService of the execution service
		// Add our data
		CyJobData jobData = dataService.addData(null, "nodes", nodeArray);
		jobData = dataService.addData(jobData, "edges", edgeArray);
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
	
	
	private HashMap<Long, String> getNetworkNodes(CyNetwork currentNetwork) {
		List<CyNode> cyNodeList = currentNetwork.getNodeList();
		
		HashMap<Long, String> nodeMap = new HashMap<>();
		for (CyNode node : cyNodeList) {
			String nodeName = currentNetwork.getRow(node).get(CyNetwork.NAME, String.class);
			nodeMap.put(node.getSUID(), nodeName);
		}
		
		return nodeMap;
	}
	
	private List<List<String>> getNetworkEdges(CyNetwork currentNetwork, Map<Long, String> nodeMap) {
		CyTable edgeTable = currentNetwork.getDefaultEdgeTable();
		System.out.println("Edge columns: " + edgeTable.getColumns());
		List<CyEdge> cyEdgeList = currentNetwork.getEdgeList();
		
		List<List<String>> edgeArray = new ArrayList<>();
		for (CyEdge edge : cyEdgeList) {
			List<String> sourceTargetWeight = new ArrayList<>();
			
			CyNode source = edge.getSource();
			CyNode target = edge.getTarget();
			String sourceName = nodeMap.get(source.getSUID());
			sourceTargetWeight.add(sourceName);
			String targetName = nodeMap.get(target.getSUID());
			sourceTargetWeight.add(targetName); 
			
			Double weight = currentNetwork.getRow(edge).get("weight", Double.class);
			
			if (weight == null) {
				sourceTargetWeight.add("1");
			} else {
				sourceTargetWeight.add(String.valueOf(weight));
			}
			
			edgeArray.add(sourceTargetWeight);
		}
		
		return edgeArray;
	}
	
	
}

