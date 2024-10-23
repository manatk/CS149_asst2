/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    
    this->counter = 0;
    this->total_tasks = 0;
    this->tasks_completed = 0;
    this->num_threads = num_threads;
    this->threads_sleeping = 0;
    this->stop = false;

    for (int i = 0; i < num_threads; i++){
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerThread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    stop = true;

    if (threads_sleeping > 0){   
         cv.notify_all();      // wake up sleeping threads when all runs done
    }
    
    for (auto& thread: threads){
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    std::unique_lock<std::mutex> lock(mtx);
    
    total_tasks = num_total_tasks;
    counter = 0;
    tasks_completed = 0;
    cur_runnable = runnable;

    if (threads_sleeping > 0){
        cv.notify_all();          // wake up sleeping threads after tasks intialized
    }
     
    while (tasks_completed < total_tasks){
        cv_finished.wait(lock);  //wait until last task finishes
    }
    
    cur_runnable = nullptr;
}

void TaskSystemParallelThreadPoolSleeping::workerThread(){
    
    while (true) {
        int temp = 0;
        
        std::unique_lock<std::mutex> lock(mtx);

        while ((counter >= total_tasks || cur_runnable == nullptr) && !stop){
            threads_sleeping++;
            cv.wait(lock);         // wait until more tasks available
            threads_sleeping--;
        }

        if (stop){
           break;
        }
        
         temp = counter;
         counter++;
         
         lock.unlock();
         cur_runnable->runTask(temp, total_tasks);
         
         
         tasks_completed++;
         
         if (tasks_completed >= total_tasks) {
             cv_finished.notify_all(); // wake up run after last task finishes
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