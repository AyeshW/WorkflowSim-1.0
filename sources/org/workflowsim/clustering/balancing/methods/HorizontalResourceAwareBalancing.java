package org.workflowsim.clustering.balancing.methods;

import org.workflowsim.Task;
import org.workflowsim.clustering.TaskSet;

import java.util.*;

/**
 * @author randika
 */
public class HorizontalResourceAwareBalancing extends BalancingMethod {
    /**
     * Initialize a HorizontalImpactBalancing object
     * @param levelMap the level map
     * @param taskMap the task map
     * @param clusterNum the clusters.num
     */
    public HorizontalResourceAwareBalancing(Map levelMap, Map taskMap, int clusterNum) {
        super(levelMap, taskMap, clusterNum);
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Map<Integer, List<TaskSet>> map = getLevelMap();
        for (List<TaskSet> taskList : map.values()) {
            process(taskList);
        }

    }

    /**
     * Sort taskSet based on their impact factors and then merge similar taskSet together
     * @param taskList
     */
    public void process(List<TaskSet> taskList) {

        if (taskList.size() > getClusterNum()) {
            List<TaskSet> jobList = new ArrayList<>();
            for (int i = 0; i < getClusterNum(); i++) {
                jobList.add(new TaskSet());
            }
            int clusters_size = taskList.size() / getClusterNum();
            if(clusters_size * getClusterNum() < taskList.size()){
                clusters_size ++;
            }
            double coreAvg = getAverageCores(taskList);
            double runtimeAvg = getAverageRunTime(taskList);
            sortListDecreasing(taskList);
            for (TaskSet set : taskList) {
                //sortListIncreasing(jobList);
                //Log.printLine(set.getJobRuntime());
                TaskSet job = null;
                try{
                    job = getCandidateTastSet(jobList, set, clusters_size, coreAvg, runtimeAvg);
                }catch(Exception e) {
                    e.printStackTrace();
                }
                addTaskSet2TaskSet(set, job);
                job.addTask(set.getTaskList());
                job.setImpactFafctor(set.getImpactFactor());
                //update dependency
                for (Task task : set.getTaskList()) {
                    getTaskMap().put(task, job);//this is enough
                }
            }
            System.out.println(".....................");
            System.out.println(calculateCoreHourWastage(jobList));
            taskList.clear();
        }
    }

    /**
     * Sort taskSet in an ascending order of impact factor
     * @param taskList taskSets to be sorted
     */
    private void sortListIncreasing(List taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            @Override
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
                return (int) (t1.getJobRuntime() - t2.getJobRuntime());

            }
        });

    }
    /**
     * Sort taskSet in a descending order of impact factor
     * @param taskList taskSets to be sorted
     */
    private void sortListDecreasing(List taskList) {

        Collections.sort(taskList, new Comparator<TaskSet>() {
            @Override
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
                if(Math.abs(t2.getImpactFactor() - t1.getImpactFactor()) > 1.0e-8){
                    if(t1.getImpactFactor() > t2.getImpactFactor()){
                        return 1;
                    }else if(t1.getImpactFactor() < t2.getImpactFactor()){
                        return -1;
                    }else{
                        return 0;
                    }
                }
                else{
                    if (t1.getJobRuntime() > t2.getJobRuntime()){
                        return 1;
                    }else if (t1.getJobRuntime() < t2.getJobRuntime()) {
                        return -1;
                    }else{
                        return 0;
                    }
                }

            }
        });

    }

    private List<TaskSet> getNextPotentialTaskSets(List<TaskSet> taskList,
                                                   TaskSet checkSet, int clusters_size, double coreAvg, double runtimeAvg){

        Map<Double, List<TaskSet>> map = new HashMap<>();

        Map<Double, List<TaskSet>> testMap = getClusteringFactors(taskList, checkSet, coreAvg, runtimeAvg);

        List<Double> facKeys = new ArrayList<Double>(testMap.keySet());

        Collections.sort(facKeys);

        List<TaskSet> potentialClusters = new ArrayList<>();

        for(double facKey : facKeys){
            List<TaskSet> clusters = testMap.get(facKey);
            for(TaskSet cluster : clusters){
                if(cluster.getTaskList().size() < clusters_size){
                    potentialClusters.add(cluster);
                    break;
                }
            }
            if(potentialClusters.size()>0){
                break;
            }
        }

        return potentialClusters;

    }

    private Map<Double, List<TaskSet>> getClusteringFactors(List<TaskSet> clusters, TaskSet taskSet, double coreAvg, double runtimeAvg){

        Map<Double, List<TaskSet>> map = new HashMap<>();
        Task task = taskSet.getTaskList().get(0);
        for(TaskSet set: clusters){
            double factor = (0.1 * Math.abs(task.getCores()-getMaxCore(set, coreAvg))) * (0.1 * Math.abs(task.getCloudletLength()-getTotalRunTime(set, runtimeAvg)));
            if(!map.containsKey(factor)){
                map.put(factor, new ArrayList<>());
            }
            List<TaskSet> list = map.get(factor);
            if(!list.contains(set)){
                list.add(set);
            }
        }
        return map;

    }

    private double getTotalRunTime(TaskSet set, double runtimeAvg){
        double totalRunTime = runtimeAvg;
        if(set.getTaskList()!=null){
            for(Task task: set.getTaskList()){
                totalRunTime+=task.getCloudletLength();
            }
        }
        return totalRunTime/1000;
    }

    private double getMaxCore(TaskSet set, double coreAvg){
        double cores = coreAvg;
        if(set.getTaskList()!=null){
            for (Task task: set.getTaskList()){
                if(cores<task.getCores()){
                    cores = task.getCores();
                }
            }
        }
        return cores;
    }

    public double getAverageRunTime(List<TaskSet> set){
        double total = 0;
        for(TaskSet taskSet: set){
            total+=taskSet.getTaskList().get(0).getCloudletLength();
        }
        return total/set.size();
    }

    public double getAverageCores(List<TaskSet> set){
        double total = 0;
        for(TaskSet taskSet: set){
            total+=taskSet.getTaskList().get(0).getCores();
        }
        return total/set.size();
    }


    /**
     * Gets the potential candidate taskSets to merge
     * @param taskList
     * @param checkSet
     * @param clusters_size
     * @return
     */
    protected TaskSet getCandidateTastSet(List<TaskSet> taskList,
                                          TaskSet checkSet,
                                          int clusters_size, double coreAvg, double runtimeAvg) {



        List<TaskSet> potential = null;
        try{
            potential=getNextPotentialTaskSets(taskList, checkSet,  clusters_size, coreAvg, runtimeAvg);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        TaskSet task = null;
        long max = Long.MIN_VALUE;
        for(TaskSet set: potential){
            if(set.getJobRuntime() > max){
                max = set.getJobRuntime();
                task = set;
            }
        }

        if (task != null) {
            return task;
        } else {
            return taskList.get(0);
        }
    }

    public double calculateCoreHourWastage(List<TaskSet> jobList){

        double coreHourWastage = 0;
        for (TaskSet cluster : jobList){
            List<Task> tasks = cluster.getTaskList();
            sortListDecreasingByCores(tasks);
            double maxCores = tasks.get(0).getCores();
            for (Task tsk : tasks){
                coreHourWastage += tsk.getCloudletLength() * (maxCores - tsk.getCores());
            }
        }
        return coreHourWastage;

    }

    public void sortListDecreasingByCores(List<Task> job){
        Collections.sort(job, new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                //Decreasing order
                return (int) (t2.getCores() - t1.getCores());
            }
        });
    }
}
