from typing import List

from shared import Executor, Job, JobDefinitionInput


def calculate_score_for_executor(
    executor: Executor, job_input: JobDefinitionInput
) -> float:
    """
    Score an executor based on how well it fits a job.
    """
    cpu_utilization = job_input.cpu_cores / executor.available_cpu_cores
    memory_utilization = job_input.memory_gb / executor.available_memory_gb

    # GPU utilization (if applicable)
    if job_input.gpu_count > 0 and executor.available_gpu_count > 0:
        gpu_utilization = job_input.gpu_count / executor.available_gpu_count
    else:
        gpu_utilization = 0.0

    # Best-Fit strategy: prefer higher utilization (tighter fit, less waste)
    # Weight the resources - GPUs are more valuable
    WEIGHTS = {
        "cpu": 1.0,
        "memory": 1.0,
        "gpu": 2.0,  # GPUs are scarce, prioritize efficient use
    }

    # Calculate weighted average utilization
    if job_input.gpu_count > 0:
        # GPU job: consider all three resources
        score = (
            cpu_utilization * WEIGHTS["cpu"]
            + memory_utilization * WEIGHTS["memory"]
            + gpu_utilization * WEIGHTS["gpu"]
        ) / sum(WEIGHTS.values())
    else:
        # CPU-only job: only consider CPU and memory
        score = (
            cpu_utilization * WEIGHTS["cpu"] + memory_utilization * WEIGHTS["memory"]
        ) / (WEIGHTS["cpu"] + WEIGHTS["memory"])

    return score


def can_executor_fit_job(executor: Executor, job_input: JobDefinitionInput) -> bool:
    """
    Check if an executor has sufficient resources to run a job.

    Returns True if executor can fit the job, False otherwise.
    """

    # Check CPU capacity
    if executor.available_cpu_cores < job_input.cpu_cores:
        return False

    # Check memory capacity
    if executor.available_memory_gb < job_input.memory_gb:
        return False

    # Check GPU requirements (if job needs GPU)
    if job_input.gpu_count > 0:
        # GPU job: MUST have matching GPU type and sufficient count
        if (
            executor.gpu_type != job_input.gpu_type
            or executor.available_gpu_count < job_input.gpu_count
        ):
            return False

    # Respect the requested data center, if any
    if job_input.data_center and executor.data_center != job_input.data_center:
        return False

    # Executor passes all checks
    return True


def bin_packing_best_worker(
    job_input: JobDefinitionInput, list_of_all_executors: List[Executor]
) -> Executor | None:
    """
    Take a job input and a list of candidate executors (already filtered to
    the right data center by the caller) and return the best worker for the
    job, or None if nothing currently fits.

    This is a pure scoring function: it does not mutate the executor or job -
    assignment is a separate, explicitly Raft-replicated state transition.
    """
    suitable_executors = [
        executor
        for executor in list_of_all_executors
        if can_executor_fit_job(executor, job_input)
    ]
    if not suitable_executors:
        return None

    scored_executors = [
        (calculate_score_for_executor(executor, job_input), executor)
        for executor in suitable_executors
    ]
    best_executor: Executor | None = max(scored_executors, key=lambda x: x[0])[1]
    return best_executor


def calculate_score_for_job(job: Job, executor: Executor) -> float:
    """
    Score a job based on how well it fits an executor.
    Higher score = better fit (more resource utilization, less waste)
    """
    job_input = JobDefinitionInput(
        docker_image=job.docker_image,
        command=job.command,
        cpu_cores=job.cpu_cores,
        memory_gb=job.memory_gb,
        gpu_type=job.gpu_type,
        gpu_count=job.gpu_count,
        data_center=job.data_center,
    )
    return calculate_score_for_executor(executor, job_input)


def can_job_fit_executor(job: Job, executor: Executor) -> bool:
    """
    Check if a job can fit on an executor.
    """
    job_input = JobDefinitionInput(
        docker_image=job.docker_image,
        command=job.command,
        cpu_cores=job.cpu_cores,
        memory_gb=job.memory_gb,
        gpu_type=job.gpu_type,
        gpu_count=job.gpu_count,
        data_center=job.data_center,
    )
    return can_executor_fit_job(executor, job_input)


def bin_packing_best_job(
    pending_jobs: List[Job], list_of_executors: List[Executor]
) -> List[tuple]:
    """
    Reverse bin-packing: given available executors, find the best pending jobs.
    Maximizes resource utilization across the cluster.

    Returns a list of (job, executor) pairs that maximize resource utilization.
    Uses greedy algorithm: repeatedly find the best job-executor match.
    """
    assignments = []

    # Create working copies to track available resources
    available_executors = [ex for ex in list_of_executors]
    unassigned_jobs = pending_jobs.copy()

    # Greedy algorithm: repeatedly find best match until no more matches possible
    while available_executors and unassigned_jobs:
        best_match = None
        best_score = -1.0

        # Find the best (job, executor) pair across all combinations
        # This maximizes resource utilization globally
        for job in unassigned_jobs:
            for executor in available_executors:
                if can_job_fit_executor(job, executor):
                    score = calculate_score_for_job(job, executor)
                    if score > best_score:
                        best_score = score
                        best_match = (job, executor)

        # If we found a match, assign it and update available resources
        if best_match:
            job, executor = best_match
            assignments.append(best_match)

            # Remove assigned job and executor from consideration
            unassigned_jobs.remove(job)
            available_executors.remove(executor)
        else:
            # No more valid matches possible
            break

    return assignments


def bin_packing_best_job_for_executor(pending_jobs: List[Job], executor: Executor):
    """
    Find the single best pending job for a specific executor.
    Useful when a single executor becomes available.

    Returns the job that best utilizes the executor's resources, or None.
    """
    suitable_jobs = [job for job in pending_jobs if can_job_fit_executor(job, executor)]
    if not suitable_jobs:
        return None

    scored_jobs = [
        (calculate_score_for_job(job, executor), job) for job in suitable_jobs
    ]
    _, best_job = max(scored_jobs, key=lambda x: x[0])
    return best_job
