package hasim;

import org.apache.log4j.Logger;

import eduni.simjava.Sim_predicate;
import eduni.simjava.Sim_type_p;

import gridsim.GridSimTags;

/**
 * Logger for this class
 */
public enum HTAG {
	END_OF_SIMULATION_TAG,
	outside,
	fileWriteRequest,
	fileReadRequest, 
	WRITEFILE_COMPLETE,
	READFILE_COMPLETE,

	writeDelta,
	readDelta,
	writing,
	reading,
	completeWriting,
	completeReading,

	lastTag,
	fileReadComplete,
	fileWriteComplete,
	update, 
	
	mem_write,
	mem_sort,
	mem_threshold,
	mem_full,
	mem_combine,
	mem_spill, 
	
	newtag,
	
	heartbeat,
	heartbeat_request,
	heartbeat_response,
	
	new_map_task,
	new_reduce_task,
	new_shuffle_task,
	
	
	get_split_datum_response,
	get_split_datum_request,
	
	cpu_map_response,
	mem_sort_request,
	mem_sort_complete, 
	
	cpu_local_submit,
	cpu_tic, 
	
	joblet_progress,
	joblet_complete,
	joblet_submit_local, 
	joblet_part,
	joblet_part_return,
	joblet_complete_return,
	joblet_submitPart_complete_return,
	
	datum_send,
	netlet_send_part,
	netlet_receive,
	netlet_complete, 
	netlet_part_return,
	netlet_complete_return,
	datum_send_return,   
	 
	file_submit,
	file_complete, 
	file_part,
	file_complete_return,
	file_part_return, 
	task_start_local, 
	
	mapper_block,
	mapper_unblock,
	
	datum_complete_return,
	datum_part_return,
	
	mem_add,
	mem_add_return,
	mem_set_mapper,
	mem_set_spiller,
	mem_flush, 
	spiller_start,
	mem_reset,
	spiller_end,
	
	mem_add_return_true,
	mem_add_return_false, 
	
	read_tmp_delta,
	write_tmp_delta,
	
	process_tmp_delta,
	
	send_tmp_delta,
	read_tmp_delta_return,
	write_tmp_delta_return,
	process_tmp_delta_return,
	
	send_tmp_delta_return, 
	hdd_add, 
	hdd_remove, 
	hdd_check,
	
	cpu, 
	cpu_add, 
	cpu_check, 
	
	job_tracker_add_job, 
	
	START,
	mapper_cpu, 
	one_mapper_finished, 
	one_mapper_copy_finished, reducer_start_task_local,
	netend_simple_send, 
	reducer_copy_one_map_return, 
	reducer_start, reducer_check_mappers_out, process_split, cpu_split, hdd_split, TEST_END, datum_send_init, monitor_stop,
	merge_read,merg_write, 
	
	
	
	test_1, test_2, test_1_return,	test_2_return,test_3, test_4,
	test_blocking, dfs_write, reducer_CPU_reduce, 
	
	//simotree collector
	simotree_add_job,
	simotree_add_rack,
	simotree_add_task,
	simotree_add_topology,
	simotree_clear_tasks,
	simotree_moveup_job,
	simotree_moveup_map,
	simotree_moveup_reduce,
	simotree_removeAll_children,
	simotree_remove_task,
	simotree_add_object,
	
	//
	cp_hh_hrd, cp_hh_net, 
	cp_hm_h, cp_hm_n,
	cp_mh_h, cp_mh_n, cp_add_object, 
	cp_net,cp_hard,
	cp_hard_from, cp_hard_to, 
	engine_add, 
	engine_check, 
	engine_check_return, msg_send, msg_check, sim_msg_send, n, sim_msg_receive, import_split,
	rStory_mergeInMem, hdd_read, hdd_write, reduce_HDFS_reduce, getCopyResult, START_LocalFSMerger, addMapOutputFilesOnDisk,
	START_LocalFSMerger_return,
	START_InMemFSMergeThread,doInMemMerge_return, getMapOutputReturn, all_done, doInMemMerge,
	ReduceCopierInit, add_pending_shuffle, shuffle, check_pending_shuffles, shuffle_return, all_shuffles_finished, 
	all_shuffles_finished_return, NULL, jobinfo_complete, local_jobtest, replicate, releaseTask, check_doMerge
	

	;

	public static final int END_OF_SIMULATION=-1;
	public static final int HDBASE = 1000;
	public final int id;

	public static String toString(int i){
		HTAG tag=get(i);
		return tag==HTAG.outside ? ""+i :tag.name();
	}
	public static HTAG get(int i) {
		if(i==END_OF_SIMULATION){
			return END_OF_SIMULATION_TAG;
		}
		
		int dif = i - HDBASE;
		HTAG[] arr = HTAG.values();
		if (dif < 0 || dif > arr.length)
			return HTAG.outside;
		return HTAG.values()[dif];
	}

	private HTAG() {
		this.id = ordinal() + HDBASE;
	}

	public int id() {
		return id;
	}

	public String tagName(int tagId) {
		int d = (HDBASE - tagId);
		return "" + HTAG.values()[d].name();
	}

	public Sim_predicate predicate(){
		return new Sim_type_p(id());
	}
	
	public static void main(String[] args) {
		HTAG tag = HTAG.READFILE_COMPLETE;

		System.out.println("" + tag + "=" + tag.id());
		System.out.println("reverse " + HTAG.get(tag.id()));
	}

}
