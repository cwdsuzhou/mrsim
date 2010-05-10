package hasim.gui;
import hasim.HJobTracker;
import hasim.JobInfo;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.ListModel;

import javax.swing.WindowConstants;
import javax.swing.SwingUtilities;


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
public class GuiJobTracker extends javax.swing.JFrame {
	
	// suhel 
	
	HJobTracker jt;
	
	public HJobTracker getJt() {
		return jt;
	}

	public void setJt(HJobTracker jt) {
		this.jt = jt;
	}

	private JScrollPane sclJob;
	private JList lstJ;
	private JScrollPane lstMappers;
	private JList lstMF;
	private JScrollPane scrlR;
	private JList lstR;
	private JScrollPane jScrollPane1;
	private JScrollPane jScrollPane2;
	private JButton btnTest;
	private JTextArea txt;
	private JScrollPane scrltxt;
	private JList lstJF;
	private JList lstRF;
	private JScrollPane srlMF;
	private JList lstM;
	
	DefaultListModel mdlJ=new DefaultListModel();
	DefaultListModel mdlJF=new DefaultListModel();
	DefaultListModel mdlM=new DefaultListModel();
	DefaultListModel mdlMF=new DefaultListModel();
	DefaultListModel mdlR=new DefaultListModel();
	DefaultListModel mdlRF=new DefaultListModel();


	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				GuiJobTracker inst = new GuiJobTracker();
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}
	
	public GuiJobTracker() {
		super();
		initGUI();
	}
	
	private void initGUI() {
		try {
			GridBagLayout thisLayout = new GridBagLayout();
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			thisLayout.rowWeights = new double[] {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1};
			thisLayout.rowHeights = new int[] {7, 7, 7, 7, 7, 7, 7};
			thisLayout.columnWeights = new double[] {0.1, 0.1, 0.1, 0.1, 0.1, 0.1};
			thisLayout.columnWidths = new int[] {7, 7, 7, 7, 7, 7};
			getContentPane().setLayout(thisLayout);
			{
				sclJob = new JScrollPane();
				getContentPane().add(sclJob, new GridBagConstraints(0, 0, 1, 2, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					
					lstJ = new JList(mdlJ);
					sclJob.setViewportView(lstJ);
				}
			}
			{
				lstMappers = new JScrollPane();
				getContentPane().add(lstMappers, new GridBagConstraints(1, 0, 1, 2, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					lstM = new JList(mdlM);
					lstMappers.setViewportView(lstM);
				}
			}
			{
				srlMF = new JScrollPane();
				getContentPane().add(srlMF, new GridBagConstraints(2, 0, 1, 2, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					
					lstMF = new JList(mdlMF);
					srlMF.setViewportView(lstMF);
				}
			}
			{
				scrlR = new JScrollPane();
				getContentPane().add(scrlR, new GridBagConstraints(1, 2, 1, 3, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					lstR = new JList(mdlR);
					scrlR.setViewportView(lstR);
				}
			}
			{
				jScrollPane1 = new JScrollPane();
				getContentPane().add(jScrollPane1, new GridBagConstraints(2, 2, 1, 3, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					lstRF = new JList(mdlRF);
					jScrollPane1.setViewportView(lstRF);
				}
			}
			{
				jScrollPane2 = new JScrollPane();
				getContentPane().add(jScrollPane2, new GridBagConstraints(0, 2, 1, 3, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					lstJF = new JList(mdlJF);
					jScrollPane2.setViewportView(lstJF);
				}
			}
			{
				scrltxt = new JScrollPane();
				getContentPane().add(scrltxt, new GridBagConstraints(3, 0, 3, 5, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					txt = new JTextArea();
					scrltxt.setViewportView(txt);
					txt.setText("info");
				}
			}
			{
				btnTest = new JButton();
				getContentPane().add(btnTest, new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
				btnTest.setText("test");
				btnTest.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent evt) {
						btnTestActionPerformed(evt);
					}
				});
			}
			pack();
			this.setSize(400, 400);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void updateJobTracker(HJobTracker jt){
		mdlJ.removeAllElements();
		for (JobInfo job : jt.jobsRunning) {
			mdlJ.addElement(job);
		}
		for (JobInfo job : jt.jobsWaiting) {
			mdlJ.addElement(job);
		}
		
		mdlJF.removeAllElements();
		for (JobInfo job : jt.jobsFinished) {
			mdlJF.addElement(job);
		}
		
		
		
	}
	
	private void btnTestActionPerformed(ActionEvent evt) {
		System.out.println("btnTest.actionPerformed, event="+evt);
		updateJobTracker(null);
	}

}
