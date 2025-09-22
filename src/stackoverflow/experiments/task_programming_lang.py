from clearml import Logger, Task, Dataset, TaskTypes
import yaml
import os
import polars as pl
import matplotlib.pyplot as plt

def add_languages_to_plot(lf: pl.LazyFrame, lang:str):
    lang_df = lf.filter(pl.col("tag") == lang).collect()
    plt.plot(lang_df["year"], lang_df["num_quest"], label=lang)


if __name__ == "__main__":
    with open('params.yaml', 'r') as file:
        config = yaml.safe_load(file)

    task = Task.init(project_name=config["CLEARML"]["EXPERIMENTS"]["PROJECT_NAME"],
                    task_name=config["CLEARML"]["EXPERIMENTS"]["TASK_PROGRAMING_LANG"],
                    task_type=TaskTypes.data_processing)
    
    task.execute_remotely()

    print("Gather datasets")
    ds_quest = Dataset.get(dataset_name=config["DATASETS"]["QUEST_NAME"],
                     alias=config["DATASETS"]["QUEST_NAME"])
    quest_path = os.path.join(ds_quest.get_local_copy(), "merged","*.parquet")
    df_quest = pl.scan_parquet(quest_path)

    ds_tags = Dataset.get(dataset_name=config["DATASETS"]["TAGS_NAME"],
                     alias=config["DATASETS"]["TAGS_NAME"])
    tags_path = os.path.join(ds_tags.get_local_copy(), "merged", "*.parquet")
    df_tags = pl.scan_parquet(tags_path)

    print("Backend langs")
    #filter the programming languages
    bck_langs = ["c#", "java", "python", "node.js", "php"]
    df_tags_prog = df_tags.filter(pl.col("tag").is_in(bck_langs))

    backend = df_quest.select("id", pl.col("creationdate").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").dt.year().alias("year"))\
                      .join(df_tags_prog, on="id")\
                      .group_by("tag", "year")\
                      .agg(num_quest=pl.col("id").count())\
                      .sort("year", descending=False)
    
    for lang in bck_langs:
        add_languages_to_plot(backend, lang)

    plt.xlabel("Years")
    plt.ylabel("Number of questions")
    plt.title('Number of questions per year for backend programming languages')
    plt.legend(loc='upper left')
    plt.show()
                      
    print("frontend framework")

    bck_langs = ["jquery", "angular", "react"]
    df_tags_prog = df_tags.filter(pl.col("tag").str.contains_any(bck_langs))\
                         .with_columns(pl.when(pl.col("tag").str.contains("jquery")).then(pl.lit("jquery"))\
                                         .when(pl.col("tag").str.contains("angular")).then(pl.lit("angular"))\
                                         .otherwise(pl.lit("react"))\
                                         .alias("framework"))

    frontend = df_quest.select("id", pl.col("creationdate").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").dt.year().alias("year"))\
                      .join(df_tags_prog, on="id")\
                      .group_by("framework", "year")\
                      .agg(num_quest=pl.col("id").count())\
                      .select("num_quest", "year", tag=pl.col("framework"))\
                      .sort("year", descending=False)
    
    for lang in bck_langs:
        add_languages_to_plot(frontend, lang)

    plt.xlabel("Years")
    plt.ylabel("Number of questions")
    plt.title('Number of questions per year for fronted framework')
    plt.legend(loc='upper left')
    plt.show()


    print("DBs")

    bck_langs = ["oracle", "sql-server", "mysql", "mongodb", "postgres"]
    df_tags_prog = df_tags.filter(pl.col("tag").str.contains_any(bck_langs))\
                         .with_columns(pl.when(pl.col("tag").str.contains("oracle")).then(pl.lit("oracle"))\
                                         .when(pl.col("tag").str.contains("sql-server")).then(pl.lit("sql-server"))\
                                         .when(pl.col("tag").str.contains("mysql")).then(pl.lit("mysql"))\
                                         .when(pl.col("tag").str.contains("mongodb")).then(pl.lit("mongodb"))\
                                         .otherwise(pl.lit("postgres"))\
                                         .alias("db"))

    frontend = df_quest.select("id", pl.col("creationdate").str.to_datetime("%Y-%m-%d %H:%M:%S%#z").dt.year().alias("year"))\
                      .join(df_tags_prog, on="id")\
                      .group_by("db", "year")\
                      .agg(num_quest=pl.col("id").count())\
                      .select("num_quest", "year", tag=pl.col("db"))\
                      .sort("year", descending=False)
    
    for lang in bck_langs:
        add_languages_to_plot(frontend, lang)

    plt.xlabel("Years")
    plt.ylabel("Number of questions")
    plt.title('Number of questions per year for Database')
    plt.legend(loc='upper left')
    plt.show()