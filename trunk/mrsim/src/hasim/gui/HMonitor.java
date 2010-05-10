package hasim.gui;

import org.apache.log4j.Logger;

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



@SuppressWarnings("serial")
public class HMonitor extends javax.swing.JFrame {
	/**
	 * Logger for this class
	 */
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(HMonitor.class);

	public static enum DebugMode{NONE,SLEEP, SLEEP_STEP,STEP}
	
	
	
	private static DebugMode debugMode=DebugMode.NONE;

	public static DebugMode getDebugMode() {
		return debugMode;
	}

	public static void setDebugMode(DebugMode debugMode) {
		HMonitor.debugMode = debugMode;
	}

	private static int SLEEP=1000;

	public static int getSLEEP() {
		return SLEEP;
	}

	public static void setSLEEP(int sLEEP) {
		SLEEP = sLEEP;
	}

	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				HMonitor inst = new HMonitor("test monitor");
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	};
	
	private JButton btnStep;
	private JCheckBox chck;
	private JPanel pnl;
	
	private JScrollPane scrlMonitor;
	private JScrollPane scrltxt;
	private JTextArea txt;
	private Color defultColor,alarmColor;
	private JLabel lblLine;
	
	public boolean debug=true;
	
	LockTread lockThead=new LockTread();
	
	private PanelKeyValue lastPanelKeyValue;

	Map<String, PanelKeyValue> panels=new LinkedHashMap<String, PanelKeyValue>();
	

	public HMonitor(String name) {
		super(name);
		setTitle(name);
		initGUI();
		myInitGui();
		setDefaultCloseOperation(DISPOSE_ON_CLOSE);
	}

	//////
	
	public PanelKeyValue addMobject(String mo){
		PanelKeyValue pnlKeyValue=panels.get(mo);

		if(pnlKeyValue ==null){
			pnlKeyValue=new PanelKeyValue(mo);
			pnl.add(pnlKeyValue);
			pnlKeyValue.setVisible(true);
			pnl.setVisible(true);
			pnl.updateUI();

			panels.put(mo, pnlKeyValue);
		}
		return pnlKeyValue;
	}
	
	public void block(){
		if(! isVisible()){
			setVisible(true);

			//TODO check later it may block all the application
		}
		requestFocus();
		
		btnStep.setBackground(alarmColor);
		btnStep.requestFocus();

		//assert lock.isLocked();
		try{
			lockThead.lock.lock();
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			lockThead.lock.unlock();
		}
		btnStep.setBackground(defultColor);

	}
	
	private void btnStepActionPerformed(ActionEvent evt) {
		lockThead.click=true;
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
			this.setFocusTraversalPolicyProvider(true);
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
	
	public void log(String mo, double time,String value, boolean append){
		if(debugMode==DebugMode.NONE)return;

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
	private void myInitGui(){
		defultColor=btnStep.getBackground();
		alarmColor=Color.RED;

		lockThead.start();

	}
	private void sleep(int time){
		try {
			Thread.sleep(time);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void step(){
		if(debugMode==DebugMode.NONE)return;

		step("step");
	}
	
	public void step(double time, String msg){
		if(debugMode==DebugMode.NONE)return;

		if(msg!=null)txt(time, msg);
		
		if(debugMode==DebugMode.NONE || debug==false)return;

		int lineNumber=Thread.currentThread().getStackTrace()[2].getLineNumber();
		lblLine.setText(""+lineNumber);
		if(debugMode != DebugMode.SLEEP)txt(time, msg);

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
		if(debugMode==DebugMode.NONE)return;

		step(0.0,msg);
	}
	
	private void thisComponentShown(ComponentEvent evt) {
		btnStep.setBackground(Color.YELLOW);
	}
	
	public void txt(double time, String msg){
		if(debugMode==DebugMode.NONE)return;
		
		if(time==0 && msg==null)return;
		txt.append("\n"+time+"\t:"+msg);
		if(chck.isSelected())
			txt.setCaretPosition(txt.getDocument().getLength());
	}
}

class LockTread extends Thread{
	public boolean click=false;
	public ReentrantLock lock=new ReentrantLock(true);

	public boolean running=true;
	public void run() {
		assert !lock.isLocked();
		
		lock.lock();
		
		while(running){
			
			while(! click){
				try{
				Thread.sleep(100);
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			lock.unlock();
			lock.lock();
			click=!click;
		}
	};
};