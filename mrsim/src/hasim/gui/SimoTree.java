package hasim.gui;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_system;

import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HMapperStory;
import hasim.HMapperTask;
import hasim.HReducerStory;
import hasim.HReducerTask;
import hasim.HStory;
import hasim.HTaskTracker;
import hasim.HTopology;
import hasim.JobInfo;
import hasim.core.CPU;
import hasim.core.HDD;
import hasim.json.JsonRealRack;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import java.awt.Toolkit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

/**
 * This code was edited or generated using CloudGarden's Jigloo SWT/Swing GUI
 * Builder, which is free for non-commercial use. If Jigloo is being used
 * commercially (ie, by a corporation, company or business for any purpose
 * whatever) then you should purchase a license for each developer using Jigloo.
 * Please visit www.cloudgarden.com for details. Use of Jigloo implies
 * acceptance of these licensing terms. A COMMERCIAL LICENSE HAS NOT BEEN
 * PURCHASED FOR THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED LEGALLY FOR
 * ANY CORPORATE OR COMMERCIAL PURPOSE.
 */
@SuppressWarnings("serial")
public class SimoTree extends javax.swing.JPanel {
	public enum TreeIndex {
		finished, running, waiting
	}

	
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(SimoTree.class);;

	private HLogger tempHlog;
	public static SimoTree createAndShowGUI() {
		// Create and set up the window.
		SimoTree panel = new SimoTree(null);

		JFrame frame = new JFrame("Frame title");
		frame.setLayout(new BorderLayout());
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// Create and set up the content pane.
		panel.setOpaque(true); // content panes must be opaque
		frame.setContentPane(panel);

		// Display the window.
		frame.pack();
		frame.setSize(300, 400);
		frame.setVisible(true);
		return panel;
	}

	public static void main(String[] args) throws Exception {
		SimoTree simotree = createAndShowGUI();

		// javax.swing.SwingUtilities.invokeLater(new Runnable() {
		// public void run() {
		// createAndShowGUI();
		// }
		// });

		Thread.sleep(500);

		simotree.addTest("job_1");

		Thread.sleep(10000);


		Thread.sleep(10000);
	}

	boolean end_simulation = false;
	DefaultMutableTreeNode jf = new DefaultMutableTreeNode("Jobs Finished");

	Map<JobInfo, DefaultMutableTreeNode> jobs = new LinkedHashMap<JobInfo, DefaultMutableTreeNode>();
	DefaultMutableTreeNode jobTracker = new DefaultMutableTreeNode("JobTracker");

	DefaultMutableTreeNode jr = new DefaultMutableTreeNode("Jobs Running");

	DefaultMutableTreeNode jw = new DefaultMutableTreeNode("Jobs Waiting");

	Map<HMapperStory, DefaultMutableTreeNode> mappers = new LinkedHashMap<HMapperStory, DefaultMutableTreeNode>();
	DefaultMutableTreeNode mTasks = new DefaultMutableTreeNode("MTasks");
	DefaultMutableTreeNode mTasksIdle = new DefaultMutableTreeNode("MTasksIdle");

	Map<Object, DefaultMutableTreeNode> nodes = new LinkedHashMap<Object, DefaultMutableTreeNode>();

	DefaultMutableTreeNode rackes = new DefaultMutableTreeNode("Rackes");
	Map<HReducerStory, DefaultMutableTreeNode> reducers = new LinkedHashMap<HReducerStory, DefaultMutableTreeNode>();
	DefaultMutableTreeNode rTasks = new DefaultMutableTreeNode("RTasks");
	DefaultMutableTreeNode rTasksIdle = new DefaultMutableTreeNode("RTasksIdle");
	private JScrollPane scrl;

	// added lately
	DefaultMutableTreeNode tasks = new DefaultMutableTreeNode("Tasks");

	private Toolkit toolkit = Toolkit.getDefaultToolkit();

	// topology
	DefaultMutableTreeNode topologyNode = new DefaultMutableTreeNode("Topology");

	// objects
	DefaultMutableTreeNode objects = new DefaultMutableTreeNode("objects");

	public JTree tree;

	public DefaultTreeModel treeModel = new DefaultTreeModel(jobTracker);

	final JTextArea txt;
	public SimoTree(JTextArea txt) {
		this.txt = txt;
	}

	public void addJob(JobInfo job) {
		DefaultMutableTreeNode jobNode = new DefaultMutableTreeNode(job);

		DefaultMutableTreeNode mw = new DefaultMutableTreeNode(
				"Mappers Waiting");
		DefaultMutableTreeNode mr = new DefaultMutableTreeNode(
				"Mappers Running");
		DefaultMutableTreeNode mf = new DefaultMutableTreeNode(
				"Mappers Finished");

		DefaultMutableTreeNode rw = new DefaultMutableTreeNode(
				"Reducers Waiting");
		DefaultMutableTreeNode rr = new DefaultMutableTreeNode(
				"Reducers Running");
		DefaultMutableTreeNode rf = new DefaultMutableTreeNode(
				"Reducers Finished");

		jw.add(jobNode);

		jobNode.add(mw);
		jobNode.add(mr);
		jobNode.add(mf);

		jobNode.add(rw);
		jobNode.add(rr);
		jobNode.add(rf);

		nodes.put(job, jobNode);
		// nodes.put(mw, mw);
		// nodes.put(mr, mr);
		// nodes.put(mf, mf);
		// nodes.put(rw, rw);
		// nodes.put(rr, rr);
		// nodes.put(rf, rf);

		for (HStory mapper : job.mappersWaiting) {
			DefaultMutableTreeNode mNode = new DefaultMutableTreeNode(mapper);
			nodes.put(mapper, mNode);
			mw.add(mNode);
		}
		for (HStory r : job.reducersWaiting) {
			DefaultMutableTreeNode rNode = new DefaultMutableTreeNode(r);
			nodes.put(r, rNode);
			rw.add(rNode);
		}
		tree.updateUI();

	}

	public void addRack(String rckname, Map<String, HTaskTracker> trackers) {
		DefaultMutableTreeNode rack = new DefaultMutableTreeNode(rckname);
		rackes.add(rack);
		for (HTaskTracker tracker : trackers.values()) {
			DefaultMutableTreeNode trackerNode = new DefaultMutableTreeNode(
					tracker);
			rack.add(trackerNode);

			DefaultMutableTreeNode cpuNode = new DefaultMutableTreeNode(tracker
					.getCpu());
			DefaultMutableTreeNode hddNode = new DefaultMutableTreeNode(tracker
					.getHdd());
			DefaultMutableTreeNode netNode = new DefaultMutableTreeNode(tracker
					.getNetend());
			DefaultMutableTreeNode infNode = new DefaultMutableTreeNode(
					"mappers:" + tracker.getaMapperSlots() + ", reducers:"
							+ tracker.getaReducerSlots());

			trackerNode.add(cpuNode);
			trackerNode.add(hddNode);
			trackerNode.add(netNode);
			trackerNode.add(infNode);

		}

		tree.updateUI();

	}

	public void addTask(Object mt) {
		DefaultMutableTreeNode taskNode = nodes.get(mt);
		if (taskNode == null) {
			taskNode = new DefaultMutableTreeNode(mt);
			nodes.put(mt, taskNode);
		}

		if (mt instanceof HMapperTask) {
			mTasks.add(taskNode);
		} else if (mt instanceof HReducerTask) {
			rTasks.add(taskNode);
		}
	}

	public void addTest(String jobName) {
		DefaultMutableTreeNode jobNode = new DefaultMutableTreeNode(jobName);

		DefaultMutableTreeNode mw = new DefaultMutableTreeNode(
				"Mappers Waiting");
		DefaultMutableTreeNode mr = new DefaultMutableTreeNode(
				"Mappers Running");
		DefaultMutableTreeNode mf = new DefaultMutableTreeNode(
				"Mappers Finished");

		DefaultMutableTreeNode rw = new DefaultMutableTreeNode(
				"Reducers Waiting");
		DefaultMutableTreeNode rr = new DefaultMutableTreeNode(
				"Reducers Running");
		DefaultMutableTreeNode rf = new DefaultMutableTreeNode(
				"Reducers Finished");

		jw.add(jobNode);

		jobNode.add(mw);
		jobNode.add(mr);
		jobNode.add(mf);

		jobNode.add(rw);
		jobNode.add(rr);
		jobNode.add(rf);

		nodes.put(jobNode, jobNode);
		nodes.put(mw, mw);
		nodes.put(mr, mr);
		nodes.put(mf, mf);
		nodes.put(rw, rw);
		nodes.put(rr, rr);
		nodes.put(rf, rf);

		for (int i = 0; i < 5; i++) {
			DefaultMutableTreeNode mNode = new DefaultMutableTreeNode("m_" + i);
			nodes.put("m__" + i, mNode);
			mw.add(mNode);
		}
		for (int i = 0; i < 3; i++) {
			DefaultMutableTreeNode mNode = new DefaultMutableTreeNode("r_" + i);
			nodes.put("r__" + i, mNode);
			mw.add(mNode);
		}

		tree.updateUI();

	}
	
	public void addObject(Object o){
		DefaultMutableTreeNode tNode = new DefaultMutableTreeNode(o);
		objects.add(tNode);
	}

	public void addTopology(HTopology topology) {
		//
		DefaultMutableTreeNode tNode = new DefaultMutableTreeNode(topology);
		topologyNode.add(tNode);
	}

	public void clearTasks() {
		mTasks.removeAllChildren();
		rTasks.removeAllChildren();
	}

	public void endSimulation() {
		this.end_simulation = true;
	}

	public Object getSelection() {
		DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree
				.getLastSelectedPathComponent();
		if (node == null)
			return null;
		Object o = node.getUserObject();
		return o;
	}

	public void init() {
		myInit();
		initGUI();

		int delay = 2000; // delay for 5 sec.
		int period = 1000; // repeat every sec.
		final Timer timer = new Timer();

		tree.updateUI();
//		timer.scheduleAtFixedRate(new TimerTask() {
//
//			@Override
//			public void run() {
//				// int v=scrl.getVerticalScrollBar().getValue();
//
//				// tree.updateUI();
//				tree.revalidate();
//
//				if (!Sim_system.running())
//					timer.cancel();
//				// scrl.getVerticalScrollBar().setValue(v);
//			}
//		}, delay, period);
	}

	private void initGUI() {
		try {
			{
				BorderLayout thisLayout = new BorderLayout();
				this.setLayout(thisLayout);
				this.setSize(300, 400);
				{
					scrl = new JScrollPane();
					this.add(scrl, BorderLayout.NORTH);
					{
						tree = new JTree(treeModel);
						scrl.getViewport().add(tree);
						// scrl.setViewportView(tree);
						BorderLayout treeLayout = new BorderLayout();
						tree.setLayout(treeLayout);
						tree.setSize(300, 400);
						tree.setEditable(true);
						tree.setAutoscrolls(true);
						tree
								.addTreeSelectionListener(new TreeSelectionListener() {
									public void valueChanged(
											TreeSelectionEvent evt) {
										treeValueChanged(evt);
									}
								});
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// public void update(){
	// tree.updateUI();
	//
	// }

	

	public void moveUpJobInfo(JobInfo jobinfo) {
		DefaultMutableTreeNode node = nodes.get(jobinfo);
		assert node != null;

		DefaultMutableTreeNode parentToRemove = (DefaultMutableTreeNode) node
				.getParent();
		int moveIndex = parentToRemove.getParent().getIndex(parentToRemove);

		DefaultMutableTreeNode parentToAdd = (DefaultMutableTreeNode) node
				.getParent().getParent().getChildAt(moveIndex + 1);

		parentToRemove.remove(node);
		parentToAdd.add(node);

	}

	public void moveUpMappersStory(HMapperStory story) {
			
		try {
			
		DefaultMutableTreeNode node = nodes.get(story);
		assert node != null;

		DefaultMutableTreeNode parentToRemove = (DefaultMutableTreeNode) node
				.getParent();
		int moveIndex = parentToRemove.getParent().getIndex(parentToRemove);

		DefaultMutableTreeNode parentToAdd = (DefaultMutableTreeNode) node
				.getParent().getParent().getChildAt(moveIndex + 1);

		parentToRemove.remove(node);
		parentToAdd.add(node);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void moveUpReducerStory(HReducerStory story) {
		DefaultMutableTreeNode node = nodes.get(story);
		assert node != null;

		DefaultMutableTreeNode parentToRemove = (DefaultMutableTreeNode) node
				.getParent();
		int moveIndex = parentToRemove.getParent().getIndex(parentToRemove);

		DefaultMutableTreeNode parentToAdd = (DefaultMutableTreeNode) node
				.getParent().getParent().getChildAt(moveIndex + 1);

		parentToRemove.remove(node);
		parentToAdd.add(node);

	}

	private void myInit() {
		jobTracker.insert(jw, jobTracker.getChildCount());
		jobTracker.insert(jr, jobTracker.getChildCount());
		jobTracker.insert(jf, jobTracker.getChildCount());

		nodes.put(jw, jw);
		nodes.put(jr, jr);
		nodes.put(jf, jf);

		jobTracker.add(rackes);

		// added recently
		jobTracker.add(tasks);
		tasks.add(mTasks);
		tasks.add(mTasksIdle);
		tasks.add(rTasks);
		tasks.add(rTasksIdle);

		//
		jobTracker.add(topologyNode);

		jobTracker.add(objects);
	}

	public void refereshSimoTree() {
		if(tempHlog != null)
			txt.setText("\n" + tempHlog);
		tree.updateUI();
	}

	public void removeAllChildren() {

		jw.removeAllChildren();
		jr.removeAllChildren();
		jf.removeAllChildren();

		rackes.removeAllChildren();

		mTasks.removeAllChildren();
		mTasksIdle.removeAllChildren();

		rTasks.removeAllChildren();
		rTasksIdle.removeAllChildren();

		topologyNode.removeAllChildren();
		
		objects.removeAllChildren();
	}

	public boolean removeTask(Object mt) {
		DefaultMutableTreeNode node = nodes.get(mt);
		assert mt != null;
		// if(nodes.get(mt)==null)return false;
		nodes.get(mt).removeFromParent();

		if (mt instanceof HMapperTask) {
			mTasksIdle.add(node);
		} else if (mt instanceof HReducerTask) {
			rTasksIdle.add(node);
		}
		return true;
	}

	private void treeValueChanged(TreeSelectionEvent evt) {
		// DefaultMutableTreeNode
		// node=(DefaultMutableTreeNode)tree.getLastSelectedPathComponent();
		Object o = getSelection();
		if (o instanceof HLoggerInterface) {
			tempHlog=((HLoggerInterface) o).getHlog();
			txt.setText("\n" + tempHlog);
		}

	}

}