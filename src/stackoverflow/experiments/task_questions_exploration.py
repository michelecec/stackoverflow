from clearml import Logger, Task, Dataset, TaskTypes
import yaml
import os
import polars as pl
import matplotlib.pyplot as plt

if __name__ == "__main__":
    with open('params.yaml', 'r') as file:
        config = yaml.safe_load(file)

    task = Task.init(project_name=config["CLEARML"]["EXPERIMENTS"]["PROJECT_NAME"],
                    task_name=config["CLEARML"]["EXPERIMENTS"]["TASK_QUEST_EXPLORATION"],
                    task_type=TaskTypes.data_processing)
    
    #task.execute_remotely()

    print("Gather datasets")
    ds_quest = Dataset.get(dataset_name=config["DATASETS"]["QUEST_NAME"],
                     alias=config["DATASETS"]["QUEST_NAME"])
    quest_path = os.path.join(ds_quest.get_local_copy(), "merged","*.parquet")

    df_quest = pl.scan_parquet(quest_path)

    #stats on Creation date
    quest_stats = df_quest.select(pl.col("creationdate").str.to_datetime("%Y-%m-%d %H:%M:%S%#z"))\
                          .select(pl.min("creationdate").alias("MIN Question creation date"), 
                                  pl.median("creationdate").alias("MEDIAN Question creation date"), 
                                  pl.mean("creationdate").alias("MEAN Question creation date"), 
                                  pl.max("creationdate").alias("MAX Question creation date"))\
                            .collect()

    task.upload_artifact("Question creation statistics", quest_stats.head().to_pandas())

    Logger.current_logger().report_table(
        title="Question statistics", 
        series="Question statistics", 
        table_plot=quest_stats.head().to_pandas()
    )

    #number of questions per year

    quest_yearly = df_quest.select(year=pl.col("creationdate").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").dt.year(), 
                                   id=pl.col("id"))\
                            .group_by(pl.col("year"))\
                            .agg(num_quest=pl.col("id").count())\
                            .collect()
   
    plt.bar(quest_yearly["year"], quest_yearly["num_quest"], color='skyblue', edgecolor='black')

    # Adding labels and title
    plt.xlabel('years')
    plt.ylabel('Number of questions')
    plt.title('Number of questions per year')
    plt.show()


    #most used tags

    ds_tags = Dataset.get(dataset_name=config["DATASETS"]["TAGS_NAME"],
                     alias=config["DATASETS"]["TAGS_NAME"])
    tags_path = os.path.join(ds_tags.get_local_copy(), "merged", "*.parquet")
    df_tags = pl.scan_parquet(tags_path)
   
    tot_tags = df_quest.select("id")\
                        .join(df_tags, on="id")\
                        .group_by("tag")\
                        .agg(num_quest=pl.col("id").count())\
                        .sort("num_quest", descending=True)\
                        .head(15).collect()

    bar_colors = ['tab:red', 'tab:blue', 'tab:red', 'tab:orange']

    plt.bar(tot_tags["tag"], tot_tags["num_quest"], color=bar_colors)

    # Adding labels and title
    plt.xlabel('Tags used')
    plt.ylabel('Number of questions')
    plt.title('Most used tags')
    plt.show()
    



    