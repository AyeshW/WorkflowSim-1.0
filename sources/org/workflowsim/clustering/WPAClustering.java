package org.workflowsim.clustering;

import java.util.*;

import org.workflowsim.Job;
import org.workflowsim.Task;

public class WPAClustering extends BasicClustering
{
    /**
     * The number of clustered jobs per level.
     */
    private final int clusterNum;
    /**
     * The number of tassk in a job.
     */
    private final int clusterSize;
    /**
     * The map from depth to tasks at that depth.
     */
    private final Map<Integer, List> mDepth2Task;

    private int maxDepth;

    /**
     * Initialize a HorizontalClustering Either clusterNum or clusterSize should
     * be set
     *
     * @param clusterNum clusters.num
     * @param clusterSize clusters.size
     */
    public WPAClustering(int clusterNum, int clusterSize) {
        super();
        this.clusterNum = clusterNum;
        this.clusterSize = clusterSize;
        this.mDepth2Task = new HashMap<>();
        this.maxDepth = 0;

    }

    /**
     * The main function
     */
    @Override
    public void run() {

        for (Iterator it = getTaskList().iterator(); it.hasNext();) {
            Task task = (Task) it.next();
//            System.out.println(task.getDepth());
            int depth = task.getDepth();
            if (!mDepth2Task.containsKey(depth)) {
                mDepth2Task.put(depth, new ArrayList<>());
            }
            List list = mDepth2Task.get(depth);
            if (!list.contains(task)) {
                list.add(task);
            }
            this.maxDepth = task.getDepth();
        }

//        System.out.println(mDepth2Task.entrySet().toString());

        List<Task> tasksAtMaxDepth = mDepth2Task.get(maxDepth);
        addTasks2Job(tasksAtMaxDepth);
        WPA();
        updateDependencies();
        addClustDelay();


    }

    /**
     * Merges tasks into a fixed number of jobs.
     */
    private void bundleClustering() {

        for (Map.Entry<Integer, List> pairs : mDepth2Task.entrySet()) {
            List list = pairs.getValue();

            long seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));
            seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));

            int num = list.size();
            int avg_a = num / this.clusterNum;
            int avg_b = avg_a;
            if (avg_a * this.clusterNum < num) {
                avg_b++;
            }

            int mid = num - this.clusterNum * avg_a;
            if (avg_a <= 0) {
                avg_a = 1;
            }
            if (avg_b <= 0) {
                avg_b = 1;
            }
            int start = 0, end = -1;
            for (int i = 0; i < this.clusterNum; i++) {
                start = end + 1;
                if (i < mid) {
                    //use avg_b
                    end = start + avg_b - 1;
                } else {
                    //use avg_a
                    end = start + avg_a - 1;

                }


                if (end >= num) {
                    end = num - 1;
                }
                if (end < start) {
                    break;
                }
                addTasks2Job(list.subList(start, end + 1));
            }


        }
    }

    /**
     * Merges a fixed number of tasks into a job
     */
    private void collapseClustering() {
        for (Map.Entry<Integer, List> pairs : mDepth2Task.entrySet()) {
            List list = pairs.getValue();

            long seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));
            seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));

            int num = list.size();
            int avg = this.clusterSize;

            int start = 0;
            int end = 0;
            int i = 0;
            do {
                start = i * avg;
                end = start + avg - 1;
                i++;
                if (end >= num) {
                    end = num - 1;
                }
                Job job = addTasks2Job(list.subList(start, end + 1));
            } while (end < num - 1);

        }
    }

    private void WPA(){
        for(int i = this.maxDepth; i>1; i--){
//            System.out.println(i);
            List<Task> tasksAtLevel = mDepth2Task.get(i);
            sortTasksByLongestParent(tasksAtLevel);
            for(Task t: tasksAtLevel){
                assignParentsToClusters(t);
            }

        }
    }

    private Task getLongestParent(Task t){
        List<Task> parents = t.getParentList();
        Task longestParent = parents.get(0);
        for(int i=0; i<parents.size(); i++){
            if (longestParent.getCloudletLength()<parents.get(i).getCloudletLength()){
                longestParent = parents.get(i);
            }
        }
        return longestParent;
    }

    private void sortTasksByLongestParent(List<Task> tasks){
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                //Decreasing order
                return (int) (getLongestParent(t1).getCloudletLength() - getLongestParent(t2).getCloudletLength());
            }
        });
    }

    private List<Task> getUnassignedTasks(List<Task> tasks){
//        System.out.println(tasks.size());
        List<Task> unAssignedTasks = new ArrayList<Task>();
        for(Task t: tasks){
            if(!t.isAssigned()){
                unAssignedTasks.add(t);
            }
        }
//        System.out.println(unAssignedTasks.size());
        return unAssignedTasks;
    }

    private void sortTaskByDescendingRunTime(List<Task> tasks){
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                //Decreasing order
                return (int) (t2.getCloudletLength() - t1.getCloudletLength());
            }
        });
    }

    private long getSumOfClusterRunTime(List<Task> tasks){
        long sum = 0;
        for (Task t : tasks){
            sum+=t.getCloudletLength();
        }
        return sum;
    }

    private void assignParentsToClusters(Task t){
        Task longestParent = getLongestParent(t);
        List<Task> cluster = new ArrayList<Task>();
        long maxRunTime = longestParent.getCloudletLength();
        if(!longestParent.isAssigned()){
            cluster.add(longestParent);
            Job job = addTasks2Job(cluster);
            longestParent.setAssigned(true);
        }
        List<Task> tasks = getUnassignedTasks(t.getParentList());
//        System.out.println(tasks.size());
        sortTaskByDescendingRunTime(tasks);
//        System.out.println(tasks.get(0).getCloudletLength());
        if(tasks.size()>0){
            cluster = new ArrayList<Task>();
            while (tasks.size()>0){
                Task tsk = tasks.get(tasks.size()-1);
                tasks.remove(tsk);
                if(getSumOfClusterRunTime(cluster)+tsk.getCloudletLength()<maxRunTime){
                    cluster.add(tsk);
                    tsk.setAssigned(true);
                    System.out.println("working");
                }else {
                    Job job = addTasks2Job(cluster);
                    cluster = new ArrayList<Task>();
                    cluster.add(tsk);
                    tsk.setAssigned(true);
                }
                if (tasks.size()==0){
                    Job job = addTasks2Job(cluster);
                    break;
                }
            }
        }
    }

}
