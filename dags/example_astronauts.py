"""
## Astronaut & Weather Correlation DAG

This DAG queries the list of astronauts currently in space and correlates it with
real-time weather data at the ISS location to provide comprehensive analytics.

### Features Demonstrated:

**TaskFlow API**: All tasks use Python functions decorated with @task, which automatically
handles dependency management and data passing between tasks.

**Dynamic Task Mapping**: The print_astronaut_craft task dynamically creates a task instance
for each astronaut, adjusting based on how many people are currently in space.

**Multiple Dataset Production**: This DAG produces three datasets for downstream DAGs:
- `current_astronauts`: Updated when astronaut data is fetched from the API
- `astronaut_statistics`: Updated when statistics are calculated
- `iss_weather_data`: Updated when weather data is fetched for ISS location

**Parallel Data Processing**: Weather and statistics calculations run in parallel for efficiency.

**Data Processing Pipeline**:
1. Fetch astronaut data from Open Notify API
2. Print individual astronaut greetings (mapped tasks, runs in parallel)
3. Fetch ISS position and weather data from Open-Meteo API (runs in parallel with stats)
4. Calculate astronaut statistics (runs in parallel with weather)
5. Analyze correlation between astronaut activity and weather conditions
6. Generate comprehensive summary report with insights

**Correlation Analysis**: Calculates weather comfort index and provides insights on the
relationship between space operations and Earth weather conditions below the ISS.

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
weather_dataset = Dataset("iss_weather_data")


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

    @task(outlets=[weather_dataset])
    def get_iss_weather_data() -> dict:
        """
        This task fetches weather data for the ISS location using Open-Meteo API.
        The ISS orbits at approximately 400km altitude, so we'll fetch weather data
        for a representative location and solar activity data.
        """
        # Fetch ISS current position
        iss_response = requests.get("http://api.open-notify.org/iss-now.json")
        iss_position = iss_response.json()["iss_position"]
        latitude = float(iss_position["latitude"])
        longitude = float(iss_position["longitude"])

        # Fetch weather data for the location below the ISS
        # Using Open-Meteo API (free, no API key required)
        weather_url = "https://api.open-meteo.com/v1/forecast"
        weather_params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover",
            "temperature_unit": "fahrenheit",
        }
        weather_response = requests.get(weather_url, params=weather_params)
        weather_data = weather_response.json()

        result = {
            "iss_latitude": latitude,
            "iss_longitude": longitude,
            "temperature_fahrenheit": weather_data["current"]["temperature_2m"],
            "humidity_percent": weather_data["current"]["relative_humidity_2m"],
            "wind_speed_kmh": weather_data["current"]["wind_speed_10m"],
            "cloud_cover_percent": weather_data["current"]["cloud_cover"],
            "timestamp": weather_data["current"]["time"],
        }

        print(f"ISS Weather Data: {result}")
        return result

    @task
    def analyze_correlation(
        stats: dict, weather_data: dict, astronaut_list: list[dict]
    ) -> dict:
        """
        This task analyzes correlation between astronaut activity and weather conditions.
        It generates insights about the relationship between space activities and Earth weather.
        """
        analysis = {
            "astronaut_count": stats["total_astronauts"],
            "unique_crafts": stats["unique_crafts"],
            "weather_conditions": {
                "temperature_f": weather_data["temperature_fahrenheit"],
                "humidity": weather_data["humidity_percent"],
                "wind_speed": weather_data["wind_speed_kmh"],
                "cloud_cover": weather_data["cloud_cover_percent"],
            },
            "iss_location": {
                "latitude": weather_data["iss_latitude"],
                "longitude": weather_data["iss_longitude"],
            },
        }

        # Calculate some interesting metrics
        # Weather comfort index (simplified): higher is better
        comfort_index = (
            (80 - abs(weather_data["temperature_fahrenheit"] - 70))
            + (100 - weather_data["humidity_percent"])
            + (50 - min(weather_data["wind_speed_kmh"], 50))
            + (100 - weather_data["cloud_cover_percent"])
        ) / 4

        # Observation: More astronauts might correlate with more missions during favorable conditions
        astronauts_per_weather_score = stats["total_astronauts"] / max(comfort_index, 1)

        analysis["insights"] = {
            "weather_comfort_index": round(comfort_index, 2),
            "astronauts_per_weather_score": round(astronauts_per_weather_score, 3),
            "cloud_coverage_category": "Clear"
            if weather_data["cloud_cover_percent"] < 25
            else "Partly Cloudy"
            if weather_data["cloud_cover_percent"] < 75
            else "Overcast",
            "temperature_category": "Cold"
            if weather_data["temperature_fahrenheit"] < 50
            else "Moderate"
            if weather_data["temperature_fahrenheit"] < 80
            else "Hot",
        }

        print(f"Correlation Analysis: {analysis}")
        return analysis

    @task
    def generate_summary_report(
        stats: dict,
        astronaut_list: list[dict],
        weather_data: dict,
        correlation_analysis: dict,
    ) -> None:
        """
        This task generates a comprehensive summary report including astronaut data,
        weather conditions, and correlation analysis.
        """
        print("=" * 70)
        print("COMPREHENSIVE ASTRONAUT & WEATHER CORRELATION REPORT")
        print("=" * 70)

        print("\n🚀 ASTRONAUT STATISTICS:")
        print(f"  • Total Astronauts in Space: {stats['total_astronauts']}")
        print(f"  • Number of Different Crafts: {stats['unique_crafts']}")
        print("\n  Breakdown by Craft:")
        for craft, count in stats["crafts"].items():
            print(f"    - {craft}: {count} astronaut(s)")

        print("\n🌍 ISS LOCATION & WEATHER CONDITIONS:")
        print(
            f"  • ISS Position: {weather_data['iss_latitude']:.2f}°N, {weather_data['iss_longitude']:.2f}°E"
        )
        print(f"  • Ground Temperature: {weather_data['temperature_fahrenheit']:.1f}°F")
        print(f"  • Humidity: {weather_data['humidity_percent']:.0f}%")
        print(f"  • Wind Speed: {weather_data['wind_speed_kmh']:.1f} km/h")
        print(f"  • Cloud Cover: {weather_data['cloud_cover_percent']:.0f}%")

        print("\n📊 CORRELATION ANALYSIS:")
        insights = correlation_analysis["insights"]
        print(f"  • Weather Comfort Index: {insights['weather_comfort_index']}/100")
        print(f"  • Cloud Coverage: {insights['cloud_coverage_category']}")
        print(f"  • Temperature Category: {insights['temperature_category']}")
        print(
            f"  • Astronauts per Weather Score: {insights['astronauts_per_weather_score']}"
        )

        print("\n💡 KEY INSIGHTS:")
        if insights["weather_comfort_index"] > 70:
            print("  • Excellent weather conditions below ISS current position")
        elif insights["weather_comfort_index"] > 50:
            print("  • Moderate weather conditions below ISS current position")
        else:
            print("  • Challenging weather conditions below ISS current position")

        if stats["total_astronauts"] > 10:
            print("  • High astronaut activity in orbit - multiple missions active")
        else:
            print("  • Standard astronaut presence in orbit")

        print("\n" + "=" * 70)

    # Define task dependencies
    astronaut_data = get_astronauts()

    # Use dynamic task mapping to run the print_astronaut_craft task for each astronaut
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=astronaut_data
    )

    # Fetch weather data and calculate stats in parallel
    stats = calculate_astronaut_stats(astronaut_data)
    weather_data = get_iss_weather_data()

    # Analyze correlation between astronaut data and weather
    correlation = analyze_correlation(stats, weather_data, astronaut_data)

    # Generate comprehensive report with all data
    generate_summary_report(stats, astronaut_data, weather_data, correlation)


# Instantiate the DAG
example_astronauts()
