package hasim.gui;
import static hasim.Tools.format;
import org.apache.log4j.Logger;
import org.apache.log4j.chainsaw.Main;

import gridsim.GridSim;
import gridsim.parallel.gui.JobTypePanel;
import hasim.HJobTracker;
import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HMapperStory;
import hasim.HMapperTask;
import hasim.HReducerStory;
import hasim.HReducerTask;
import hasim.HTopology;
import hasim.JobInfo;
import hasim.RF;
import hasim.gui.HMonitor.DebugMode;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;
import hasim.json.JsonRealRack;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import javax.swing.AbstractAction;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBoxMenuItem;

import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;

import javax.swing.WindowConstants;
import javax.swing.SwingUtilities;
import javax.swing.text.html.InlineView;
import javax.xml.bind.annotation.XmlElementDecl.GLOBAL;
import javax.xml.stream.events.StartDocument;

import org.codehaus.jackson.map.ObjectMapper;
import org.jfree.data.general.DatasetChangeListener;

import sjgv.SJGV;

import addition2.JobTest;

import com.lowagie.text.pdf.codec.GifImage;

import eduni.simjava.Sim_system;


/**
 * This code was edited or generated using CloudGarden's Jigloo
 * SWT/Swing GUI Builder, which is free for non-commercial
 * use. If Jigloo is being used commercially (ie, by a corporation,
 * company or business for any purpose whatever) then you
 * should purchase a license for each developer using Jigloo.
 * Please visit www.cloudgarden.com for details.
 * Use of Jigloo implies acceptance of these licensing terms.
 * A COMMERCIAL LICENSE HAS NOT BEEN PURCHASED FOR
 * THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED
 * LEGALLY FOR ANY CORPORATE OR COMMERCIAL PURPOSE.
 */
public class GUISimulator extends javax.swing.JFrame {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(GUISimulator.class);

	private JMenuBar mnubar;
	private JMenuItem mnuOpenFile;
	private JMenu mnuStat;
	private JButton btnBulkTests;
	private JButton btnResubmitJob;
	private AbstractAction actionReferesh;
	private JButton btnReferesh;
	private JLabel lblTime;
	private JButton btnThree;
	private JButton btnStep;
	private JToolBar jToolBar1;
	private JMenuItem mnuGraphs;
	private JCheckBoxMenuItem mnuPauseResume;
	private JSeparator sep2;
	private JMenuItem mnuShowMonitor;
	private JMenuItem mnuUpdateSimoTree;
	private TxtPanel jPanel1=new TxtPanel();
	private JMenuItem mnuTestOpen;
	public SimoTree simoPanel;
	private JMenuItem mnuClear;
	private JMenu mnuInfo;
	private JMenuItem mnuSimStart;
	private JMenuItem mnuSimStop;
	private JCheckBoxMenuItem mnuModeSleep;
	private JCheckBoxMenuItem mnuModeStep;
	private JSeparator sep1;
	private JMenu mnuSimulation;
	private JMenu File;
	private JTextArea txt;

	HSimulator sim;
	private String lastOpenJob="/home/hadoop/ms/jobs";
	//RF.firstFileInDir("data/json/job");

	public GUISimulator() {
		initGUI();

		logger.info("done");
	}

	private void initGUI() {
		try {
			GridBagLayout thisLayout = new GridBagLayout();
			getContentPane().setLayout(thisLayout);
			this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			this.addWindowListener(new WindowAdapter() {
				public void windowClosed(WindowEvent evt) {					
					if(HSimulator.running())sim.stopSimulator();
					
					sim.jobTracker.monitor.dispose();
					
					System.exit(0);
				}
			});
			{
				txt=jPanel1.getTextArea();
				simoPanel = new SimoTree(txt);
				BorderLayout simoPanelLayout = new BorderLayout();
				getContentPane().add(simoPanel, new GridBagConstraints(0, 1, 2, 7, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				simoPanel.setLayout(simoPanelLayout);
				simoPanel.init();
			}
			{
				getContentPane().add(jPanel1, new GridBagConstraints(2, 1, 5, 7, 0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					jToolBar1 = new JToolBar();
					getContentPane().add(jToolBar1, new GridBagConstraints(0, 0, 5, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
					{
						btnStep = new JButton();
						jToolBar1.add(btnStep);
						btnStep.setText("Pause");
						btnStep.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuPauseResumeActionPerformed(evt);
								lblTime.setText(""+format(Sim_system.clock()));
								if(HSimulator.isPaused()){
									btnStep.setText("Paused");
								}else{
									btnStep.setText("Running");
								}
							}
						});
					}
					{
						btnThree = new JButton();
						jToolBar1.add(btnThree);
						btnThree.setText("Three");
					}
					{
						btnReferesh = new JButton();
						jToolBar1.add(btnReferesh);
						jToolBar1.add(getBtnResubmitJob());
						jToolBar1.add(getBtnBulkTests());
						btnReferesh.setText("Refresh");
						btnReferesh.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								simoPanel.refereshSimoTree();
								lblTime.setText(""+format(Sim_system.clock()));
								
							}
						});
					}
					{
						lblTime = new JLabel();
						jToolBar1.add(lblTime);
						lblTime.setText("Time");
						lblTime.setPreferredSize(new java.awt.Dimension(115, 15));
					}
				}
			}
			thisLayout.rowWeights = new double[] {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1};
			thisLayout.rowHeights = new int[] {7, 7, 7, 7, 7, 7, 7, 7};
			thisLayout.columnWeights = new double[] {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1};
			thisLayout.columnWidths = new int[] {7, 7, 7, 7, 7, 7, 7};
			{
				mnubar = new JMenuBar();
				setJMenuBar(mnubar);
				{
					File = new JMenu();
					mnubar.add(File);
					File.setText("File");
					{
						mnuOpenFile = new JMenuItem();
						File.add(mnuOpenFile);
						mnuOpenFile.setText("Open");
						mnuOpenFile.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed O"));
						mnuOpenFile.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuOpenFileActionPerformed(evt);
							}
						});
					}
					{
						mnuTestOpen = new JMenuItem();
						File.add(mnuTestOpen);
						mnuTestOpen.setText("Test Batch Jobs");
						mnuTestOpen.setBounds(48, 19, 118, 23);
						mnuTestOpen.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed T"));
						mnuTestOpen.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuTestOpenActionPerformed(evt);
							}
						});
					}
				}
				{
					mnuSimulation = new JMenu();
					mnubar.add(mnuSimulation);
					mnuSimulation.setText("Simulation");
					{
						mnuSimStart = new JMenuItem();
						mnuSimulation.add(mnuSimStart);
						mnuSimStart.setText("Start");
						mnuSimStart.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								try {
									mnuSimStartActionPerformed(evt);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
					}
					{
						mnuSimStop = new JMenuItem();
						mnuSimulation.add(mnuSimStop);
						mnuSimStop.setText("Stop");
						//						mnuSimStop.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed S"));
						mnuSimStop.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuSimStopActionPerformed(evt);
							}
						});
					}
					{
						sep1 = new JSeparator();
						mnuSimulation.add(sep1);
						sep1.setBounds(48, 21, 70, 7);
					}

					{
						mnuModeSleep = new JCheckBoxMenuItem();
						mnuSimulation.add(mnuModeSleep);
						mnuModeSleep.setText("Sleep");
						mnuModeSleep.setSelected(false);
						mnuModeSleep.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuModeSleepActionPerformed(evt);
							}
						});
					}
					{
						mnuModeStep = new JCheckBoxMenuItem();
						mnuSimulation.add(mnuModeStep);
						mnuModeStep.setText("Step");
						mnuModeStep.setSelected(true);
						mnuModeStep.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed S"));

						mnuModeStep.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuModeSleepActionPerformed(evt);							}
						});
					}
					{
						sep2 = new JSeparator();
						mnuSimulation.add(sep2);
					}
					{
						mnuPauseResume = new JCheckBoxMenuItem();
						mnuSimulation.add(mnuPauseResume);
						mnuPauseResume.setText("Pause");
						mnuPauseResume.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed P"));
						mnuPauseResume.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuPauseResumeActionPerformed(evt);
							}
						});
					}
				}
				{
					mnuInfo = new JMenu();
					mnubar.add(mnuInfo);
					mnuInfo.setText("Info");
					{
						mnuUpdateSimoTree = new JMenuItem();
						mnuInfo.add(mnuUpdateSimoTree);
						mnuUpdateSimoTree.setText("refresh");
						mnuUpdateSimoTree.setBounds(58, 40, 54, 21);
						mnuUpdateSimoTree.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed R"));
						mnuUpdateSimoTree.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								simoPanel.refereshSimoTree();
								lblTime.setText(""+format(Sim_system.clock()));
							}
						});
					}
					{
						mnuClear = new JMenuItem();
						mnuInfo.add(mnuClear);
						mnuClear.setText("Clear");
						mnuClear.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed L"));
						mnuClear.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								txt.setText("");
							}
						});
					}
					{
						mnuShowMonitor = new JMenuItem();
						mnuInfo.add(mnuShowMonitor);
						mnuShowMonitor.setText("Show Monitor");
						mnuShowMonitor.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								if(sim.jobTracker == null)return;

								sim.jobTracker.monitor.setVisible(true);
							}
						});
					}

				}
				{
					mnuStat = new JMenu();
					mnubar.add(mnuStat);
					mnuStat.setText("Stat");
					{
						mnuGraphs = new JMenuItem();
						mnuStat.add(mnuGraphs);
						mnuGraphs.setText("Graphs");
						mnuGraphs.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuGraphsActionPerformed(evt);
							}
						});
					}
				}
			}
			pack();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private void mnuOpenFileActionPerformed(ActionEvent evt) {
		if(! HSimulator.running())return;
		JFileChooser chooser=new JFileChooser(lastOpenJob);
		int ack=chooser.showOpenDialog(this);
		if(ack != JFileChooser.APPROVE_OPTION)return;
		try {
			lastOpenJob=chooser.getSelectedFile().getAbsolutePath();
			sim.submitJob(lastOpenJob);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void mnuModeSleepActionPerformed(ActionEvent evt) {
		boolean sleep=mnuModeSleep.isSelected();
		boolean step=mnuModeStep.isSelected();

		if(sleep && step){
			HMonitor.setDebugMode(DebugMode.SLEEP_STEP);
			return;
		}
		if(sleep){
			HMonitor.setDebugMode(DebugMode.SLEEP);
			return;
		}
		if(step){
			HMonitor.setDebugMode(DebugMode.STEP);
			return;
		}
		HMonitor.setDebugMode(DebugMode.NONE);
	}

	private void mnuSimStartActionPerformed(ActionEvent evt) throws Exception {

		String rackFile=RF.firstFileInDir("data/json/rack");

		//		String configFile="data/json/config.json";
		if(HSimulator.running()){
			sim.stopSimulator();

		}

		JsonRealRack jsonRack=JsonJob.read(rackFile, JsonRealRack.class);
//		JsonConfig jsonConfig=JsonJob.read(configFile,JsonConfig.class);

		HSimulator.initSimulator(jsonRack.isFlowType());

		//copy rack file to the result dir
		RF.copy(rackFile, RF.get(sim.resultDir, RF.jsonRack));

		sim=new HSimulator();

		sim.jobTracker=new HJobTracker("JobTracker", rackFile,simoPanel);


		HMonitor.setDebugMode(DebugMode.STEP);
		HMonitor.setSLEEP(600);

		simoPanel.removeAllChildren();
//		sim.jobTracker.monitor.setVisible(true);

		//			sim=new HTopology(rackFile, flowType);
		//			
		//			sim.jobTracker.setTopology(sim.topology);


		sim.jobTracker.createEntities(sim.resultDir);
		//			sim.topology.createTopology();


		
		new Thread(){ public void run() {
				sim.start();}}.start();
//		sim.start();
				logger.info("simulator has started simulator");
	}

	private void mnuSimStopActionPerformed(ActionEvent evt) {
		if(HSimulator.running()){
			logger.info("going to stop simulator");
			sim.stopSimulator();
		}
		//Workbench.stopSimulation();
	}



	public void log(String s){
		txt.append("\n"+s);
	}

	private void mnuTestOpenActionPerformed(ActionEvent evt) {
		if(! HSimulator.running())return;
		try {
			//sim.jobTracker.submitJob("data/json/job.json");
			String fileName=RF.firstFileInDir("data/json/job");
			sim.submitJob(fileName);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void mnuPauseResumeActionPerformed(ActionEvent evt) {
		HSimulator.pauseResume();
		simoPanel.refereshSimoTree();
		lblTime.setText(""+format(Sim_system.clock()));
	}


	public static void main(String[] args) {
		(new GUISimulator()).setVisible(true);
	}

	private void mnuGraphsActionPerformed(ActionEvent evt) {
		String gfile=RF.get(sim.resultDir, RF.graph);
//		String dir="graphs/";
//		String[] files=new File(dir).list();
//		Arrays.sort(files);
//		String gfile=dir+ files[files.length-1];
		logger.debug(gfile);
		SJGV.callMain(gfile);

	}
	
	private AbstractAction getActionReferesh() {
		if(actionReferesh == null) {
			actionReferesh = new AbstractAction("Referesh", null) {
				public void actionPerformed(ActionEvent evt) {
					simoPanel.refereshSimoTree();
					lblTime.setText(""+format(Sim_system.clock()));


				}
				
			};
		}
		return actionReferesh;
	}
	
	private JButton getBtnResubmitJob() {
		if(btnResubmitJob == null) {
			btnResubmitJob = new JButton();
			btnResubmitJob.setText("Re Submit");
			btnResubmitJob.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent evt) {
					sim.submitJob(lastOpenJob);
					logger.info("job submitted "+ lastOpenJob);
				}
			});
		}
		return btnResubmitJob;
	}
	
	private JButton getBtnBulkTests() {
		if(btnBulkTests == null) {
			btnBulkTests = new JButton();
			btnBulkTests.setText("Bulk Tests");
			btnBulkTests.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent evt) {
					JobTest jobtest=new JobTest("jt"+System.nanoTime(), sim,
							lastOpenJob, 5);
					sim.jobTracker.getHUser().submitJobTest(jobtest);
					logger.info("jobtest created");
				}
			});
		}
		return btnBulkTests;
	}

}
