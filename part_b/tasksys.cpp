#include "tasksys.h"
#include "iostream"


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
   cv.notify_all();
   lock.unlock();
  
   for (auto& thread: threads){
       thread.join();
   }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
  
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                   const std::vector<TaskID>& deps) {
   TaskID task_id;
   std::unique_lock<std::mutex> batch_lock(batch_mtx);
   task_id = next_task_id++;
   batch_lock.unlock();     

   std::unique_lock<std::mutex> info_lock(info_mtx);
   tasksCompletedPerBatch[task_id] = 0;
   info_lock.unlock();
   
   if (deps.empty()) {
       for (int i = 0; i < num_total_tasks; i++) {
	        mtx.lock();
           ready_queue.push(std::make_tuple(runnable, task_id, i, num_total_tasks));
	       mtx.unlock();
       }
       cv.notify_all();
   }
   else {
       int unfinished_dependencies = 0;
       {
           std::unique_lock<std::mutex> depsLock(dependency_mtx);
           for (TaskID dependency: deps) {
               if (finishedTasks.find(dependency) == finishedTasks.end()) {
                   unfinished_dependencies++;
                   waiting_map[dependency].push_back(std::make_tuple(runnable, task_id, num_total_tasks));
               }
           }
           if (unfinished_dependencies > 0) {
               dependency_counter[task_id] = unfinished_dependencies;
           }
       }

       if (unfinished_dependencies == 0) {
           std::unique_lock<std::mutex> lock(mtx);
           for (int i = 0; i < num_total_tasks; i++) {
               ready_queue.push(std::make_tuple(runnable, task_id, i, num_total_tasks));
           }
           cv.notify_all();
       }
   }
   return task_id;
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
   while (true) {
       std::unique_lock<std::mutex> lock(mtx);
       
       while (ready_queue.empty() && !stop) {
           cv.wait(lock);
       }
       
       if (stop) {
       	  lock.unlock();
           break;
       }

       auto task = ready_queue.front();
       ready_queue.pop();

       lock.unlock();
       
       IRunnable* cur_runnable = std::get<0>(task);
       int cur_task = std::get<2>(task);
       int total_tasks = std::get<3>(task);
       TaskID taskID = std::get<1>(task);
       
       cur_runnable->runTask(cur_task, total_tasks);

       std::unique_lock<std::mutex> info_lock(info_mtx);

       tasksCompletedPerBatch[taskID]++;
       if (tasksCompletedPerBatch[taskID] == total_tasks) {
           info_lock.unlock();
           updateDependency_Queue(taskID);
       } else {
	 info_lock.unlock();
       }
   }
}

void TaskSystemParallelThreadPoolSleeping::updateDependency_Queue(TaskID completed_task) {
    
   std::vector<std::tuple<IRunnable*, TaskID, int>> tasksToSchedule;
   
   {
       std::unique_lock<std::mutex> depsLock(dependency_mtx);
       finishedTasks.insert(completed_task);
       
       auto it = waiting_map.find(completed_task);
       
       if (it != waiting_map.end()) {
           for (auto& taskTuple: it->second) {
               TaskID waitingTaskID = std::get<1>(taskTuple);
               dependency_counter[waitingTaskID]--;
               
               if (dependency_counter[waitingTaskID] == 0) {
                   tasksToSchedule.push_back(taskTuple);
                   dependency_counter.erase(waitingTaskID);
               }
           }
           waiting_map.erase(it);
       }
   }
      
   for (const auto& taskTuple : tasksToSchedule) {
           IRunnable* current = std::get<0>(taskTuple);
           TaskID waitingTaskID = std::get<1>(taskTuple);
           int total_tasks = std::get<2>(taskTuple);
           
           std::unique_lock<std::mutex> lock(mtx);
           for (int i = 0; i < total_tasks; i++) {
               
               ready_queue.push(std::make_tuple(current, waitingTaskID, i, total_tasks));
           }
           lock.unlock();
   }

       
   if (!tasksToSchedule.empty()) {
           cv.notify_all();
   }

   {
       std::unique_lock<std::mutex> batch_lock(batch_mtx);
       num_batches_done++;
       if (num_batches_done >= next_task_id) {
           cv_finished.notify_all();
       }
   }
  
}

void TaskSystemParallelThreadPoolSleeping::sync() {
   std::unique_lock<std::mutex> batch_lock(batch_mtx);
   while(num_batches_done < next_task_id) {
       cv_finished.wait(batch_lock);
   }
}
