#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <set>
#include <map>
#include <unordered_map>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    
    private:
    int max_threads;
    std:: atomic<int> task_count;
    std::mutex mtx;
    void workerThread(IRunnable* runnable, int num_total_tasks);
   
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
private:
    
    std::vector<std::thread> threads;
   
    IRunnable* cur_runnable;
    std::atomic<int> total_tasks;
    
    std::mutex mtx;
    
    void workerThread();
    std::atomic<int> counter;
    std::atomic<int> tasks_completed;
    bool stop = false;
    
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        void updateDependency_Queue(TaskID);
        void workerThread();
        int num_batches_done;
    
        int num_threads;
        std::vector<std::thread> threads;
        std::mutex mtx;
        std::mutex dependency_mtx;
    //std::mutex depsLock;
        std::condition_variable cv;           // wake up sleeping threads
        std::condition_variable cv_finished;  // tells sync when all batches are done
        int stop = false;

        std::set<TaskID> finishedTasks;
        int threads_sleeping;
        

        IRunnable* cur_runnable;
        int next_task_id;
    int tasksCompleted; 
    // number of batches

    //std::map<TaskID, std::tuple<IRunnable*, int, int, int>> runnable_map;   //maps taskID to runnable args
    
    
    //std::queue<TaskID> ready_tasks;
    //std::map<TaskID, std::set<TaskID>> waiting_tasks;
    //std::unordered_map<TaskID, std::vector<TaskID>> reversed_dependency_mapping;
    std::unordered_map<TaskID, int> dependency_counter;
    //std::queue<IRunnable*, int> ready_queue;
    std::queue<std::tuple<IRunnable*, TaskID, int, int>> ready_queue;
    std::unordered_map<TaskID, std::vector<std::tuple<IRunnable*, TaskID, int>>> waiting_map;
    std::map<TaskID, int> tasksCompletedPerBatch;

    
};
#endif
