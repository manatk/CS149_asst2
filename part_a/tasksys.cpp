#include "tasksys.h"
#include <thread>
#include <stdio.h>
#include <atomic>
#include <mutex>
#include <functional>
#include <chrono>

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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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

    this->max_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    
    std::vector<std::thread> threads;
    task_count = 0;

    for (int i = 0; i<this->max_threads;i++){
        threads.push_back(std::thread(&TaskSystemParallelSpawn::workerThread,this,runnable,num_total_tasks));
    }
    
    for (auto& thread: threads){
        thread.join();
    }
    
}

void TaskSystemParallelSpawn::workerThread(IRunnable* runnable, int num_total_tasks){
    int cur_task = 0;
    
    while (true){
        
         mtx.lock();
        
         if (task_count >= num_total_tasks){
             mtx.unlock();
             break;
         }

         cur_task = task_count;
         task_count++;
         
         mtx.unlock();
         
         runnable->runTask(cur_task, num_total_tasks);
    }
    
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
 
    //create all worker threads in constructor
    
    this->counter = 0;
    this->total_tasks = 0;
    this->tasks_completed = 0;

    for (int i = 0; i < num_threads; i++){
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::workerThread, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    
    stop = true;
    
    for (auto& thread: threads){
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
   
    mtx.lock();
    
    total_tasks = num_total_tasks;
    counter = 0;
    tasks_completed = 0;
    cur_runnable = runnable;
    
    mtx.unlock();

    while (true){
        mtx.lock();
        
        if (tasks_completed >= total_tasks){
            mtx.unlock();
            break; // done with current run when all tasks complete
        }
        mtx.unlock();
    }
    
    cur_runnable = nullptr;
   
}
void TaskSystemParallelThreadPoolSpinning::workerThread(){

    
    while (true) {

        if (stop){
            break;
        }
        
        int temp = 0;

	mtx.lock();
         if (counter >= total_tasks || cur_runnable == nullptr){
             mtx.unlock();
             continue;
         }
         
         temp = counter;
         counter++;

         mtx.unlock();

         cur_runnable->runTask(temp, total_tasks);

         tasks_completed++;
         
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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
    cur_runnable = NULL;      
    tasks_remaining = 0;
    total_tasks = 0;
    tasks_completed = 0;
    num_threads = num_threads;
    stop = false;

    for (int i = 0; i < num_threads; i++){
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerThread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    mtx.lock();
    stop = true;

    tasks_remaining = 1;
    mtx.unlock();
       
    cv.notify_all();      // wake up sleeping threads when all runs done
    
    for (auto& thread: threads){
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    cur_runnable = runnable;
  
    mtx.lock();
    total_tasks = num_total_tasks;
    tasks_remaining = num_total_tasks;
    tasks_completed = 0;
    mtx.unlock();

    cv.notify_all();
    
    std::unique_lock<std::mutex> lock(mtx);

    while (tasks_completed < total_tasks){
        cv_finished.wait(lock);  //wait until last task finishes
    }
    lock.unlock();
}

void TaskSystemParallelThreadPoolSleeping::workerThread(){
    
    while (true) {
       std::unique_lock<std::mutex> lock(mtx);

       while (tasks_remaining <= 0){
            cv.wait(lock);   
       }

        if (stop){
	  lock.unlock();
           break;
        }

        int batch_size = 2;
	if (tasks_remaining < 32){
	  batch_size = 1;
	}
	
	int task_count = total_tasks - tasks_remaining;
	int temp_total = total_tasks;
	tasks_remaining -= batch_size;
         
         lock.unlock();
	   cur_runnable->runTask(task_count, temp_total);
	   if (batch_size==2){
	     
	        cur_runnable->runTask(task_count+1, temp_total);
	   }
	   
	   lock.lock();
         tasks_completed += batch_size;
         
         if (tasks_completed >= total_tasks) {
	   lock.unlock();
             cv_finished.notify_all(); // wake up run after last task finishes
         }else{
	   lock.unlock();
	 }
         
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
