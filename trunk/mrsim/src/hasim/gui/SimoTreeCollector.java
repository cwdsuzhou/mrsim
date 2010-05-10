package hasim.gui;

import org.apache.log4j.Logger;

import dfs.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import hasim.HLogger;
import hasim.HLoggerInterface;
import hasim.HMapperStory;
import hasim.HReducerStory;
import hasim.HTAG;
import hasim.HTaskTracker;
import hasim.HTopology;
import hasim.JobInfo;
import eduni.simjava.Sim_entity;
import eduni.simjava.Sim_event;
import eduni.simjava.Sim_system;

public class SimoTreeCollector extends Sim_entity 
implements HLoggerInterface{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(SimoTreeCollector.class);

	private List<Pair<HTAG, Object>> defferedCollects=new  ArrayList<Pair<HTAG,Object>>();
	
	public SimoTreeCollector(String name, SimoTree simotree){
		super(name);
//		assert simotree != null;
		this.simotree=simotree;
		hlog=new HLogger(name);

		
	}

	final HLogger hlog;
	final private SimoTree simotree;

	public SimoTree getSimotree() {
		return simotree;
	}

	public void collect(HTAG tag, Object object){
		if(! Sim_system.running()){
			defferedCollects.add(new Pair<HTAG, Object>(tag, object));
		}
		sim_schedule(get_id(), 0.0,
				tag.id(), object);
//		logger.info("collect:"+ tag.toString());
	}

	@Override
	public HLogger getHlog() {
		return hlog;
	}

	@Override
	public void body() {
//		assert simotree != null;
		for (Pair<HTAG, Object> c : defferedCollects) {
			sim_schedule(get_id(), 0.0,
					c.getK().id(), c.getV());
		}
		
		while (Sim_system.running()) {

			Sim_event ev=new Sim_event();
			sim_get_next(ev);
			int tag= ev.get_tag();

			if( tag == HTAG.END_OF_SIMULATION){
				hlog.info("END_OF_SIMULATION");
				logger.info(get_name()+" END_OF_SIMULATION "+ Sim_system.clock());

				break;
			}
			
			if( simotree == null)
				continue;
			
			
			if(tag == HTAG.simotree_add_job.id()){
				simotree.addJob((JobInfo)ev.get_data());
				continue;
			}
			
			if(tag == HTAG.simotree_add_rack.id()){
				simotree.addRack("rack_1", (Map<String, HTaskTracker>)ev.get_data());
				logger.info("add rack "+ ev.get_data());
				continue;
			}
			
			if(tag == HTAG.simotree_add_task.id()){
				simotree.addTask(ev.get_data());
				continue;
			}
			if(tag == HTAG.simotree_add_topology.id()){
				simotree.addTopology((HTopology)ev.get_data());
				continue;
			}
			if(tag == HTAG.simotree_clear_tasks.id()){
				simotree.clearTasks();
				continue;
			}
			
			if(tag == HTAG.simotree_moveup_job.id()){
				simotree.moveUpJobInfo((JobInfo) ev.get_data());
				continue;
			}
			
			if(tag == HTAG.simotree_moveup_map.id()){
//				logger.debug(ev.get_data());
				simotree.moveUpMappersStory((HMapperStory) ev.get_data());
				continue;
			}
			
			if(tag == HTAG.simotree_moveup_reduce.id()){
				simotree.moveUpReducerStory((HReducerStory) ev.get_data());
				continue;
			}
			
			if(tag == HTAG.simotree_remove_task.id()){
				simotree.removeTask(ev.get_data());
				continue;
			}
			
			if(tag == HTAG.simotree_add_object.id()){
				simotree.addObject(ev.get_data());
				continue;
			}
			
			
			
			
			

		}

	}

	public void stopEntity() {
		sim_schedule(get_id(), 0.0, HTAG.END_OF_SIMULATION);
	}
}
