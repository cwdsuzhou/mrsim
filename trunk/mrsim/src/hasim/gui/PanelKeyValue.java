package hasim.gui;

import org.apache.log4j.Logger;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.BorderFactory;
import javax.swing.JFormattedTextField;

import javax.swing.WindowConstants;
import javax.swing.border.BevelBorder;
import javax.swing.border.LineBorder;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;


@SuppressWarnings("serial")
public class PanelKeyValue extends javax.swing.JPanel {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(PanelKeyValue.class);

	private JLabel lbl;
	private JTextField txt;
	private JFrame frameChart;
	private JFreeChart chart;
	private Color defaultColor, alarmColor;
	private XYSeries series;


	/**
	* Auto-generated main method to display this 
	* JPanel inside a new JFrame.
	*/
	public static void main(String[] args) {
		JFrame frame = new JFrame();
		frame.getContentPane().add(new PanelKeyValue(""));
		frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		frame.pack();
		frame.setVisible(true);
	}
	public PanelKeyValue(){
		initGUI("");
		 series= new XYSeries("");
	};
	public PanelKeyValue(String lblTitle) {
		
		super();
		initGUI(lblTitle);
		
		 series= new XYSeries(lblTitle);
	}
	
	private void initGUI(String lblTitle) {
		try {
			this.setPreferredSize(new java.awt.Dimension(228, 31));
			this.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
			{
				lbl = new JLabel();
				this.add(lbl);
				lbl.setText(lblTitle);
				lbl.setPreferredSize(new java.awt.Dimension(142, 21));
				lbl.addMouseListener(new MouseAdapter() {
					public void mouseClicked(MouseEvent evt) {
						lblMouseClicked(evt);
					}
				});
			}
			{
				txt = new JTextField();
				this.add(txt);
				txt.setText("txt");
				txt.setPreferredSize(new java.awt.Dimension(58, 21));
			}
			
			{
				defaultColor=txt.getBackground();
				alarmColor=Color.YELLOW;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public boolean addPoint(double time,String value, boolean append){
		txt.setText(append?txt.getText()+value:value);
		txt.setBackground(alarmColor);

		double dValue=Double.MAX_VALUE;
		try {
			dValue=Double.valueOf(value);
		} catch (Exception e) {
			// TODO: handle exception
		}
		if(dValue == Double.MAX_VALUE)
			return false;
		if(series.getItems().size()> 40){
			series.delete(0, 0);
		}
		series.add(time, dValue);

		return true;
	}
	
	public void resetAlarm(){
		txt.setBackground(defaultColor);
	}
	private void showFrameChart(){
		if(chart==null){
			 XYSeriesCollection dataset = new XYSeriesCollection();
		     dataset.addSeries(series);
//		     chart=ChartFactory.createXYBarChart(lbl.getText(), // Title
//		                "time", // x-axis Label
//		                true, "value", // y-axis Label
//		                dataset, // Dataset
//		                PlotOrientation.VERTICAL, // Plot Orientation
//		                true, // Show Legend
//		                true, // Use tooltips
//		                false // Configure chart to generate URLs?
//		            );
		     
		     chart=ChartFactory.createScatterPlot(lbl.getText(), // Title
	                "time", // x-axis Label
	                "value", // y-axis Label
	                dataset, // Dataset
	                PlotOrientation.VERTICAL, // Plot Orientation
	                true, // Show Legend
	                true, // Use tooltips
	                false // Configure chart to generate URLs?
	            ); 
			
			ChartPanel chartPanel=new ChartPanel(chart);

			
			frameChart=new JFrame();
			frameChart.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

			frameChart.setLocationRelativeTo(this);
			frameChart.setSize(400, 300);
			frameChart.getContentPane().add(chartPanel,BorderLayout.CENTER);
		}
		
		if(frameChart != null)
			frameChart.setVisible(true);
		else
			logger.error("frameChart = null");

	}

	private void lblMouseClicked(MouseEvent evt) {
		showFrameChart();
	}

}
