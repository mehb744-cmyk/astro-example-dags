"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

### Features Demonstrated:

**TaskFlow API**: All tasks use Python functions decorated with @task, which automatically
handles dependency management and data passing between tasks.

**Dynamic Task Mapping**: The print_astronaut_craft task dynamically creates a task instance
for each astronaut, adjusting based on how many people are currently in space.

**Dataset Production**: This DAG produces two datasets that can trigger downstream DAGs:
- `current_astronauts`: Updated when astronaut data is fetched from the API
- `astronaut_statistics`: Updated when statistics are calculated

**Data Processing Pipeline**:
1. Fetch astronaut data from Open Notify API
2. Print individual astronaut greetings (mapped tasks)
3. Calculate statistics (total count, crafts breakdown)
4. Generate comprehensive summary report

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

# Define datasets for data-driven scheduling
astronauts_dataset = Dataset("current_astronauts")
astronaut_stats_dataset = Dataset("astronaut_statistics")


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[astronauts_dataset]
    )
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        r = requests.get("http://api.open-notify.org/astros.json")
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    @task(outlets=[astronaut_stats_dataset])
    def calculate_astronaut_stats(astronaut_list: list[dict]) -> dict:
        """
        This task calculates statistics about astronauts in space.
        It produces the astronaut_statistics dataset which can trigger downstream DAGs.
        """
        # Calculate statistics
        crafts = {}
        for astronaut in astronaut_list:
            craft = astronaut["craft"]
            crafts[craft] = crafts.get(craft, 0) + 1

        stats = {
            "total_astronauts": len(astronaut_list),
            "crafts": crafts,
            "unique_crafts": len(crafts),
        }

        print(f"Statistics: {stats}")
        return stats

    @task
    def generate_summary_report(stats: dict, astronaut_list: list[dict]) -> None:
        """
        This task consumes both datasets (implicitly through task dependencies)
        to generate a comprehensive summary report.
        """
        print("=" * 50)
        print("ASTRONAUT SUMMARY REPORT")
        print("=" * 50)
        print(f"Total Astronauts in Space: {stats['total_astronauts']}")
        print(f"Number of Different Crafts: {stats['unique_crafts']}")
        print("\nBreakdown by Craft:")
        for craft, count in stats["crafts"].items():
            print(f"  - {craft}: {count} astronaut(s)")
        print("=" * 50)

    # Define task dependencies
    astronaut_data = get_astronauts()

    # Use dynamic task mapping to run the print_astronaut_craft task for each astronaut
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronaut_data
    )

    # Calculate stats after getting astronaut data
    stats = calculate_astronaut_stats(astronaut_data)

    # Generate report that depends on both stats and astronaut data
    generate_summary_report(stats, astronaut_data)


# Instantiate the DAG
example_astronauts()
