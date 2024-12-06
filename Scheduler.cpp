//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

// 
// "main" scheduling algorithm created by David Bockelman, mb64566
// Github repo link: https://github.com/davidbockelman/cloudsim_eec
// 

#include "Scheduler.hpp"
#include <unordered_map>
#include <algorithm>
#include <cmath>

#define MEM_THRESHOLD 0.8
#define MEM_PENALTY 2
#define GPU_BOOST 0.5
#define CLUSTER 3

static bool migrating = false;

using namespace std;

struct Machine {
    MachineId_t id;
    unsigned mips;
    unsigned cores;
    bool gpu;
};

unordered_map<CPUType_t, vector<MachineId_t>> machine_map;
unordered_map<MachineId_t, vector<VMId_t>> machine_vms;
unordered_map<MachineId_t, vector<TaskId_t>> machine_tasks;
unordered_map<MachineId_t, bool> state_changes;
unordered_map<MachineId_t, unsigned> active_tasks_map;
unordered_map<TaskId_t, MachineId_t> task_machines;
unordered_map<MachineId_t, unordered_map<SLAType_t, unsigned>> machine_sla;


void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    unsigned total_machines = Machine_GetTotal();
    for (unsigned i = 0; i < total_machines; i++) {
        MachineInfo_t machine_info = Machine_GetInfo(i);
        machine_map[machine_info.cpu].push_back(i);
        Machine_SetState(i, S0i1);
        active_tasks_map[i] = 0;
    }
    SimOutput("Scheduler::Init(): Machine map size is " + to_string(machine_map[X86].size()), 1);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

vector<MachineId_t> sort_runtime(vector<MachineId_t> m, TaskId_t task_id) {
    vector<MachineId_t> sorted = m;
    TaskInfo_t task_info = GetTaskInfo(task_id);
    
    std::sort(sorted.begin(), sorted.end(), 
        [&task_info](MachineId_t a, MachineId_t b) {
            MachineInfo_t machine_info_a = Machine_GetInfo(a);
            MachineInfo_t machine_info_b = Machine_GetInfo(b);
            float a_cpu_util = (float)(active_tasks_map[a] + 1) / machine_info_a.num_cpus;
            float a_cpu_util_factor = a_cpu_util > 1 ? (float)(machine_sla[a][task_info.required_sla] + 1) / machine_info_a.num_cpus : 1;
            
            float b_cpu_util = (float)(active_tasks_map[b] + 1) / machine_info_b.num_cpus;
            float b_cpu_util_factor = b_cpu_util > 1 ? (float)(machine_sla[b][task_info.required_sla] + 1) / machine_info_b.num_cpus : 1;
            float a_runtime = task_info.total_instructions * a_cpu_util_factor / machine_info_a.performance[0];
            float b_runtime = task_info.total_instructions * b_cpu_util_factor / machine_info_b.performance[0];
            if (task_info.gpu_capable) {
                a_runtime *= machine_info_a.gpus ? GPU_BOOST : 1;
                b_runtime *= machine_info_b.gpus ? GPU_BOOST : 1;
            }
            if ((task_info.required_memory + machine_info_a.memory_used) / machine_info_a.memory_size > MEM_THRESHOLD) {
                a_runtime *= MEM_PENALTY;
            }
            if ((task_info.required_memory + machine_info_b.memory_used) / machine_info_b.memory_size > MEM_THRESHOLD) {
                b_runtime *= MEM_PENALTY;
            }
            if (a_runtime == b_runtime) {
                return b_cpu_util < a_cpu_util;
            }
            return a_runtime < b_runtime;
        }
    );
    return sorted;
}

int find_vm(vector<VMId_t> vms, VMType_t vm_type) {
    for (auto & vm: vms) {
        VMInfo_t vm_info = VM_GetInfo(vm);
        if (vm_info.vm_type == vm_type) {
            return vm_info.vm_id;
        }
    }
    return -1;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    //  IsGPUCapable(task_id);
    //  GetMemory(task_id);
    //  RequiredVMType(task_id);
    //  RequiredSLA(task_id);
    //  RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    // Priority_t priority = RequiredSLA(task_id) == SLA0 ? HIGH_PRIORITY : RequiredSLA(task_id) == SLA1 ? MID_PRIORITY : LOW_PRIORITY;
    Priority_t priority = HIGH_PRIORITY;
    
    vector<MachineId_t> sorted_machines = sort_runtime(machine_map[RequiredCPUType(task_id)], task_id);
    unsigned buffer_end = sorted_machines.size() > CLUSTER ? CLUSTER : sorted_machines.size();
    float min_util = INFINITY;
    MachineId_t target = sorted_machines[0];
    SimOutput("Scheduler::NewTask(): Buffer = ", 4);
    for (unsigned i = 0; i < buffer_end; i++) {
        SimOutput(to_string(sorted_machines[i]) + ", ",4);
        MachineInfo_t machine_info = Machine_GetInfo(sorted_machines[i]);
        if (machine_info.s_state != S0 && !state_changes[sorted_machines[i]]) {
            state_changes[sorted_machines[i]] = true;
            Machine_SetState(sorted_machines[i], S0);
            continue;
        }
        float util = (float)(active_tasks_map[sorted_machines[i]] + 1) / machine_info.num_cpus;
        if (util > 1) {
            util = (float)(machine_sla[sorted_machines[i]][RequiredSLA(task_id)] + 1) / machine_info.num_cpus;
        }
        if (util < min_util) {
            min_util = util;
            target = sorted_machines[i];
        }
    }
    active_tasks_map[target]++;
    task_machines[task_id] = target;
    for (unsigned i = RequiredSLA(task_id); i < NUM_SLAS; i++) {
        machine_sla[target][SLAType_t(i)]++;
    }
    if (RequiredSLA(task_id)==SLA0)
    SimOutput("Scheduler::NewTask(): SLA0 Task " + to_string((float)(active_tasks_map[target]) / Machine_GetInfo(target).num_cpus) + " util, " + to_string((float)(machine_sla[target][RequiredSLA(task_id)]) / Machine_GetInfo(target).num_cpus) + " util factor", 4);
    else
    SimOutput("Scheduler::NewTask(): Task " + to_string(task_id) + " is assigned to machine " + to_string(target) + ", which has " + to_string((float)(active_tasks_map[target]) / Machine_GetInfo(target).num_cpus) + " util", 4);
    if (Machine_GetInfo(target).s_state == S0) {
        int vm_id = find_vm(machine_vms[target], RequiredVMType(task_id));
        if (vm_id == -1) {
            vm_id = VM_Create(RequiredVMType(task_id), RequiredCPUType(task_id));
            VM_Attach(vm_id, target);
            machine_vms[target].push_back(vm_id);
        }
        
        if (IsTaskGPUCapable(task_id)) {
            string out = Machine_GetInfo(target).gpus ? "" : "non-";
            SimOutput("Scheduler::NewTask(): Assigning a GPU capable task to a " + out + "GPU-capable machine", 1);
        }
        VM_AddTask(vm_id, task_id, priority);
    } else {
        machine_tasks[target].push_back(task_id);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
    active_tasks_map[task_machines[task_id]]--;
    for (unsigned i = RequiredSLA(task_id); i < NUM_SLAS; i++) {
        machine_sla[task_machines[task_id]][SLAType_t(i)]--;
    }
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    static unsigned counts = 0;
    counts++;
    
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    SimOutput("StateChangeComplete(): State change for machine " + to_string(machine_id) + " completed at time " + to_string(time), 4);
    for (auto & task: machine_tasks[machine_id]) {
       int vm_id = find_vm(machine_vms[machine_id], RequiredVMType(task));
        if (vm_id == -1) {
            vm_id = VM_Create(RequiredVMType(task), RequiredCPUType(task));
            VM_Attach(vm_id, machine_id);
            machine_vms[machine_id].push_back(vm_id);
        }
        if (IsTaskGPUCapable(task)) {
            string out = Machine_GetInfo(machine_id).gpus ? "" : "non-";
            SimOutput("Scheduler::NewTask(): Assigning a GPU capable task to a " + out + "GPU-capable machine", 1);
        }
        VM_AddTask(vm_id, task, HIGH_PRIORITY); 
    }
    state_changes[machine_id] = false;
    machine_tasks[machine_id].clear();
}

