package hasim.gui;
import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import javax.swing.WindowConstants;
import javax.swing.border.BevelBorder;
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
public class CopyOfHMonitor extends javax.swing.JFrame {
	
	///suhel
	
	
	Map<MObject, PanelKeyValue> panels=new LinkedHashMap<MObject, PanelKeyValue>();
	ReentrantLock lock=new ReentrantLock(true);
	private Color defultColor,alarmColor;
	private PanelKeyValue lastPanelKeyValue;

	public static enum DebugMode{SLEEP,STEP, SLEEP_STEP,NONE};
	public static int SLEEP=1000;
	public static DebugMode debugMode=DebugMode.NONE;
	
	public PanelKeyValue addMobject(MObject mo){
		PanelKeyValue pnlKeyValue=panels.get(mo);

		if(pnlKeyValue ==null){
			pnlKeyValue=new PanelKeyValue(mo.name());
			pnl.add(pnlKeyValue);
			pnlKeyValue.setVisible(true);
			pnl.setVisible(true);
			pnl.updateUI();

			panels.put(mo, pnlKeyValue);
		}
		return pnlKeyValue;
	}


	public void txt(double time, String msg){
		txt.append("\n"+time+"\t:"+msg);
		if(chck.isSelected())
			txt.setCaretPosition(txt.getDocument().getLength());
	}
	
	public void log(MObject mo, double time,String value, boolean append){
		PanelKeyValue tc=panels.get(mo);

		if(tc==null){
			tc=addMobject(mo);
			pnl.updateUI();
		}

		tc.addPoint(time, value, append);
		if(lastPanelKeyValue != null) 
			lastPanelKeyValue.resetAlarm();

		lastPanelKeyValue=tc;
	}
	
	private void sleep(int time){
		try {
			Thread.sleep(time);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void step(String msg, double time){

		if(debugMode==DebugMode.NONE)return;

		int lineNumber=Thread.currentThread().getStackTrace()[2].getLineNumber();
		lblLine.setText(""+lineNumber);
		txt(time, msg);

		switch (debugMode) {
		case SLEEP:
			sleep(SLEEP);
			return;

		case STEP:

			block();
			return;

		case SLEEP_STEP:

			sleep(SLEEP);
			block();
			return;

		default:
			break;
		}
	}

	public void step(String msg){
		step(msg, 0);
	}
	public void step(){
		step("step");
	}
	public void block(){
		if(! isVisible()){
			setVisible(true);

			//TODO check later it may block all the application
			while(! lock.isLocked())
				sleep(30);
		}
		requestFocus();

		btnStep.setBackground(alarmColor);
		btnStep.requestFocus();

		//assert lock.isLocked();

		lock.lock();

		lock.unlock();

		btnStep.setBackground(defultColor);

	}

	private void myInitGui(){
		defultColor=btnStep.getBackground();
		alarmColor=Color.RED;


	}

	//////
	
	private JScrollPane scrltxt;
	private JCheckBox chck;
	private JPanel pnl;
	private JScrollPane scrlMonitor;
	private JButton btnStep;
	private JLabel lblLine;
	private JTextArea txt;

	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				CopyOfHMonitor inst = new CopyOfHMonitor("test monitor");
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}
	
	public CopyOfHMonitor(String name) {
		super(name);
		initGUI();
		myInitGui();
	}
	
	private void initGUI() {
		try {
			GridBagLayout thisLayout = new GridBagLayout();
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			thisLayout.rowWeights = new double[] {0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1};
			thisLayout.rowHeights = new int[] {7, 7, 7, 7, 7, 7, 7, 7, 7, 7};
			thisLayout.columnWeights = new double[] {0.1, 0.1, 0.1, 0.1, 0.1};
			thisLayout.columnWidths = new int[] {7, 7, 7, 7, 7};
			getContentPane().setLayout(thisLayout);
			this.setPreferredSize(new java.awt.Dimension(300, 400));
			this.addComponentListener(new ComponentAdapter() {
				public void componentShown(ComponentEvent evt) {
					thisComponentShown(evt);
				}
			});
			{
				scrltxt = new JScrollPane();
				getContentPane().add(scrltxt, new GridBagConstraints(0, 0, 5, 4, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					txt = new JTextArea();
					scrltxt.setViewportView(txt);
					txt.setText("jTextArea1");
				}
			}
			{
				lblLine = new JLabel();
				getContentPane().add(lblLine, new GridBagConstraints(0, 4, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				lblLine.setText("line");
				lblLine.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
			}
			{
				chck = new JCheckBox();
				getContentPane().add(chck, new GridBagConstraints(2, 4, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.VERTICAL, new Insets(0, 0, 0, 0), 0, 0));
				chck.setSelected(true);
			}
			{
				btnStep = new JButton();
				getContentPane().add(btnStep, new GridBagConstraints(3, 4, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				btnStep.setText("Step");
				btnStep.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent evt) {
						btnStepActionPerformed(evt);
					}
				});
			}
			{
				scrlMonitor = new JScrollPane();
				getContentPane().add(scrlMonitor, new GridBagConstraints(0, 5, 5, 5, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					pnl = new JPanel();
					BoxLayout pnlLayout = new BoxLayout(pnl, javax.swing.BoxLayout.Y_AXIS);
					scrlMonitor.setViewportView(pnl);
					pnl.setLayout(pnlLayout);
				}
				scrlMonitor.getViewport().add(pnl);
			}
			pack();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void btnStepActionPerformed(ActionEvent evt) {
		assert( lock.isLocked());
		lock.unlock();
		lock.lock();
	}
	
	private void thisComponentShown(ComponentEvent evt) {
		assert lock.isLocked()==false;

		lock.lock();

		btnStep.setBackground(Color.YELLOW);
	}

}
