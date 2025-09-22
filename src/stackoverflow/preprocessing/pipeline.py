from clearml import Task
from clearml.automation import PipelineController
import yaml
import os

def is_merge_needed(controller, node, _): 
    pipeline_nodes = controller.get_pipeline_dag()
    parent_node = pipeline_nodes[node.parents[0]]
    parent_task = parent_node.job.task
    if len(config["FILE_REPO"][parent_task.get_parameters()["General/data_name"].upper()]) > 1:
        return True
    return False

if __name__ == "__main__":
    print(f"working dir {os.getcwd()}")

    CONFIG_FILE = "params.yaml"
    with open(CONFIG_FILE) as f:
        config = yaml.safe_load(f)

    clearml_config = config["CLEARML"]
    prep_config = clearml_config["PREPROCESS"]
    git_config = config["GIT"]

    # create the pipeline
    pipe = PipelineController(
        name=prep_config["PIPELINE_NAME"],
        project=prep_config["PROJECT_NAME"],
        add_pipeline_tags=False,
        repo=git_config["GITLAB_URL"],
        repo_branch=git_config["GITLAB_BRANCH"],
    )

    pipe.set_default_execution_queue("default")

    # ACQUISITION ----------------------------------------------------------

    pipe.add_step(
        name="Answers_acquisition",
        base_task_project=prep_config["PROJECT_NAME"],
        base_task_name=prep_config["TASK_ACQUISITION"],
        cache_executed_step=True,
        parameter_override={
            "General/data_name": "answers",
        },
    )

    pipe.add_step(
        name="Questions_acquisition",
        base_task_project=prep_config["PROJECT_NAME"],
        base_task_name=prep_config["TASK_ACQUISITION"],
        cache_executed_step=True,
        parameter_override={
            "General/data_name": "questions",
        },
    )

    pipe.add_step(
        name="Tags_acquisition",
        base_task_project=prep_config["PROJECT_NAME"],
        base_task_name=prep_config["TASK_ACQUISITION"],
        cache_executed_step=True,
        parameter_override={
            "General/data_name": "tags",
        },
    )

    #MERGE --------------------------------------------------------

    pipe.add_step(
        name="Answers_merge",
        base_task_project=prep_config["PROJECT_NAME"],
        base_task_name=prep_config["TASK_MERGE"],
        parents=["Answers_acquisition"],
        cache_executed_step=True,
        parameter_override={
            "General/data_name": "answers",
        },
        pre_execute_callback=is_merge_needed,
    )

    pipe.add_step(
        name="Questions_merge",
        base_task_project=prep_config["PROJECT_NAME"],
        base_task_name=prep_config["TASK_MERGE"],
        parents=["Questions_acquisition"],
        cache_executed_step=True,
        parameter_override={
            "General/data_name": "questions",
        },
        pre_execute_callback=is_merge_needed,
    )

    pipe.add_step(
        name="Tags_merge",
        base_task_project=prep_config["PROJECT_NAME"],
        base_task_name=prep_config["TASK_MERGE"],
        parents=["Tags_acquisition"],
        cache_executed_step=True,
        parameter_override={
            "General/data_name": "tags",
        },
        pre_execute_callback=is_merge_needed,
    )

print("Start pipeline")

# for debugging purposes use local jobs DEV
#pipe.start_locally(run_pipeline_steps_locally=True)

# Starting the pipeline (in the background) PROD
pipe.start(queue="default")

print("done")

