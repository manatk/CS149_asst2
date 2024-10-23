#include "tasksys.h"
#include <iostream>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
   
    this->num_threads = num_threads;
    this->threads_sleeping = 0;
    this->num_batches_done = 0;
    this->next_task_id = 0;
    this->stop = false;

    for (int i = 0; i < num_threads; i++){
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerThread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    std::unique_lock<std::mutex> lock(mtx);
    stop = true;

    if (threads_sleeping > 0){
        cv.notify_all();
        
    }
    
    lock.unlock();
   
    for (auto& thread: threads){
        thread.join();
        
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    
    runAsyncWithDeps(runnable, num_total_tasks, {});

    sync();
    
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    //
    // TODO: CS149 students will implement this method in Part B.
    //

    //add all tasks to either ready queue or waiting queue
   
    std::unique_lock<std::mutex> lock(mtx);

    
    TaskID task_id = next_task_id;
    next_task_id++;

    //remove all dependencies that have already run
    int dep_size = deps.size();
    std::set<TaskID> new_deps;
    
    for (int i = 0 ; i < dep_size; i++) {
        if (finishedTasks.find(deps[i]) == finishedTasks.end()){
            new_deps.insert(deps[i]);
        }
    }

    // add batch to either ready or waiting queue
    if (new_deps.empty()){
        ready_tasks.push(task_id);
        
        if (threads_sleeping > 0){          //notify sleeping threads that tasks added
            cv.notify_all();
        }
        
    } else {
        waiting_tasks[task_id] = new_deps;
    }
    
    std::tuple<IRunnable*, int, int, int> new_runnable = std::make_tuple(runnable, num_total_tasks, 0, 0);
    runnable_map[task_id] = new_runnable;
    
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true){
        std::unique_lock<std::mutex> lock(mtx);

        while (ready_tasks.empty() && !stop){
            threads_sleeping++;
            cv.wait(lock);
            threads_sleeping--;
        }
        
        if (stop){
            break;
        }

        TaskID ready_id = ready_tasks.front();

        IRunnable* cur_runnable = std::get<0>(runnable_map[ready_id]);
        int cur_num_total_tasks = std::get<1>(runnable_map[ready_id]);
        int cur_task = std::get<2>(runnable_map[ready_id]);
        
        std::get<2>(runnable_map[ready_id]) += 1;
        if (cur_task + 1 >= cur_num_total_tasks){
            ready_tasks.pop();
        }
        
        lock.unlock();
        cur_runnable->runTask(cur_task, cur_num_total_tasks);
        lock.lock();
        
        std::get<3>(runnable_map[ready_id]) += 1;
        int cur_completed_tasks = std::get<3>(runnable_map[ready_id]);
        
        if (cur_completed_tasks >= cur_num_total_tasks) {   // last task in batch 
            updateDependency_Queue(ready_id);
        }
        
    }
}

// helper function that moves tasks from waiting ready queue after a dependency completes
void TaskSystemParallelThreadPoolSleeping::updateDependency_Queue(TaskID completed_task) {

    finishedTasks.insert(completed_task);

    num_batches_done++;
    
    if (num_batches_done >= next_task_id){
         cv_finished.notify_all();
    }  

    //iterate through all tasks in waiting queue
    for (auto it = waiting_tasks.begin(); it != waiting_tasks.end(); ){
        
        std::set<TaskID> dep = it->second;

        if (dep.find(completed_task) != dep.end()) {
            it->second.erase(completed_task);                       // remove from dependency list because completed

            if (it->second.empty()) {                              
                ready_tasks.push(it->first);                        // if no more depencies, put on ready list 

                if (threads_sleeping > 0){                         //notify sleeping threads that tasks added
                       cv.notify_all();
                }

                it = waiting_tasks.erase(it);
                
            } else {
                it++;
            }
        } else {
            it++;
        }
        
    }

}


void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // returns when all task batches are complete
    //

    std::unique_lock<std::mutex> lock(mtx);

    while(num_batches_done < next_task_id){
        cv_finished.wait(lock);
    }
  
}