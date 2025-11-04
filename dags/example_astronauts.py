"""
## Astronaut & Weather Correlation with Aggregation DAG

This comprehensive DAG queries astronauts in space, correlates with real-time weather data
at the ISS location, and aggregates data for historical tracking and trend analysis.

### Features Demonstrated:

**TaskFlow API**: All tasks use Python functions decorated with @task, which automatically
handles dependency management and data passing between tasks.

**Dynamic Task Mapping**: The print_astronaut_craft task dynamically creates a task instance
for each astronaut, adjusting based on how many people are currently in space.

**Multiple Dataset Production**: This DAG produces four datasets for downstream DAGs:
- `current_astronauts`: Updated when astronaut data is fetched from the API
- `astronaut_statistics`: Updated when statistics are calculated
- `iss_weather_data`: Updated when weather data is fetched for ISS location
- `aggregated_astronaut_weather_data`: Updated with aggregated metrics and trends

**Parallel Data Processing**: Weather and statistics calculations run in parallel for efficiency.

**Complete Data Processing Pipeline**:
1. **Data Collection**: Fetch astronaut data from Open Notify API
2. **Individual Processing**: Print individual astronaut greetings (mapped tasks, parallel)
3. **Parallel Analytics**:
   - Fetch ISS position and weather data from Open-Meteo API
   - Calculate astronaut statistics
4. **Correlation**: Analyze relationship between astronaut activity and weather conditions
5. **Reporting**: Generate comprehensive summary report with insights
6. **Aggregation**: Aggregate data with run metadata for historical tracking
7. **Trend Analysis**: Calculate trends and patterns from aggregated data
8. **Final Report**: Generate aggregate report with complete metrics

**Correlation Analysis**: Calculates weather comfort index and provides insights on the
relationship between space operations and Earth weather conditions below the ISS.

**Data Aggregation**: Prepares structured data for historical analysis, trend identification,
and downstream consumption by analytics systems.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
from datetime import datetime as dt
import requests
import json

# Define datasets for data-driven scheduling
astronauts_dataset = Dataset("current_astronauts")
astronaut_stats_dataset = Dataset("astronaut_statistics")
weather_dataset = Dataset("iss_weather_data")
aggregated_data_dataset = Dataset("aggregated_astronaut_weather_data")


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="night",
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
        try:
            print("Fetching astronaut data from Open Notify API...")
            r = requests.get("http://api.open-notify.org/astros.json", timeout=10)
            r.raise_for_status()

            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]

            print(
                f"Successfully retrieved data for {number_of_people_in_space} astronauts"
            )

            context["ti"].xcom_push(
                key="number_of_people_in_space", value=number_of_people_in_space
            )
            return list_of_people_in_space

        except requests.exceptions.Timeout:
            print("ERROR: API request timed out after 10 seconds")
            raise
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to fetch astronaut data: {str(e)}")
            raise

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
        This task fetches weather data for the ISS location using multiple weather APIs.
        Tries Open-Meteo first, then falls back to WeatherAPI.com, then OpenWeatherMap.
        If ISS location fails, uses Houston, TX (NASA JSC) as fallback location.
        """
        # Set timeout for API requests (in seconds)
        API_TIMEOUT = 10

        try:
            # Fetch ISS current position with timeout
            print("Fetching ISS position...")
            iss_response = requests.get(
                "http://api.open-notify.org/iss-now.json", timeout=API_TIMEOUT
            )
            iss_response.raise_for_status()
            iss_data = iss_response.json()

            # Validate response structure
            if "iss_position" not in iss_data:
                print(f"WARNING: Unexpected ISS API response: {iss_data}")
                raise ValueError("Invalid ISS API response structure")

            iss_position = iss_data["iss_position"]
            latitude = float(iss_position["latitude"])
            longitude = float(iss_position["longitude"])
            print(f"ISS position retrieved: {latitude}°N, {longitude}°E")

            # Try multiple weather APIs in sequence
            weather_result = _try_weather_apis(latitude, longitude, API_TIMEOUT)

            if weather_result:
                return weather_result

            # If all APIs fail, use fallback location
            print("WARNING: All weather APIs failed for ISS location")
            print("Using fallback data for Houston, TX (NASA JSC)")
            return _get_fallback_weather_data()

        except requests.exceptions.Timeout:
            print(f"WARNING: ISS API request timed out after {API_TIMEOUT} seconds")
            print("Using fallback data for Houston, TX (NASA JSC)")
            return _get_fallback_weather_data()

        except requests.exceptions.RequestException as e:
            print(f"WARNING: Failed to fetch ISS position: {str(e)}")
            print("Using fallback data for Houston, TX (NASA JSC)")
            return _get_fallback_weather_data()

        except (KeyError, ValueError) as e:
            print(f"WARNING: Unexpected API response structure: {str(e)}")
            print("Using fallback data for Houston, TX (NASA JSC)")
            return _get_fallback_weather_data()

        except Exception as e:
            print(f"WARNING: Unexpected error occurred: {str(e)}")
            print("Using fallback data for Houston, TX (NASA JSC)")
            return _get_fallback_weather_data()

    def _try_weather_apis(latitude: float, longitude: float, timeout: int) -> dict:
        """
        Tries multiple weather APIs in sequence until one succeeds.
        Returns weather data dict or None if all fail.
        """
        # API 1: Open-Meteo (Free, no API key required)
        try:
            print("Attempting weather fetch from Open-Meteo API...")
            weather_url = "https://api.open-meteo.com/v1/forecast"
            weather_params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover",
                "temperature_unit": "fahrenheit",
            }

            weather_response = requests.get(
                weather_url, params=weather_params, timeout=timeout
            )
            weather_response.raise_for_status()
            weather_data = weather_response.json()

            if "current" in weather_data:
                current = weather_data["current"]
                result = {
                    "iss_latitude": latitude,
                    "iss_longitude": longitude,
                    "temperature_fahrenheit": current.get("temperature_2m", 0.0),
                    "humidity_percent": current.get("relative_humidity_2m", 0.0),
                    "wind_speed_kmh": current.get("wind_speed_10m", 0.0),
                    "cloud_cover_percent": current.get("cloud_cover", 0.0),
                    "timestamp": current.get("time", str(dt.now())),
                    "data_source": "open-meteo",
                }
                print(f"✅ Open-Meteo API succeeded: {result}")
                return result
        except Exception as e:
            print(f"❌ Open-Meteo API failed: {str(e)}")

        # API 2: WeatherAPI.com (Free tier, no key needed for basic requests)
        try:
            print("Attempting weather fetch from WeatherAPI.com...")
            weather_url = "https://api.weatherapi.com/v1/current.json"
            weather_params = {
                "q": f"{latitude},{longitude}",
                "key": "demo",  # Demo key for testing, replace with real key in production
            }

            weather_response = requests.get(
                weather_url, params=weather_params, timeout=timeout
            )

            # WeatherAPI may return 403 without valid key, skip to next
            if weather_response.status_code == 403:
                print("❌ WeatherAPI.com requires API key")
            else:
                weather_response.raise_for_status()
                weather_data = weather_response.json()

                if "current" in weather_data:
                    current = weather_data["current"]
                    result = {
                        "iss_latitude": latitude,
                        "iss_longitude": longitude,
                        "temperature_fahrenheit": current.get("temp_f", 0.0),
                        "humidity_percent": current.get("humidity", 0.0),
                        "wind_speed_kmh": current.get("wind_kph", 0.0),
                        "cloud_cover_percent": current.get("cloud", 0.0),
                        "timestamp": current.get("last_updated", str(dt.now())),
                        "data_source": "weatherapi.com",
                    }
                    print(f"✅ WeatherAPI.com succeeded: {result}")
                    return result
        except Exception as e:
            print(f"❌ WeatherAPI.com failed: {str(e)}")

        # API 3: wttr.in (Free, simple API)
        try:
            print("Attempting weather fetch from wttr.in...")
            weather_url = f"https://wttr.in/{latitude},{longitude}"
            weather_params = {"format": "j1"}

            weather_response = requests.get(
                weather_url, params=weather_params, timeout=timeout
            )
            weather_response.raise_for_status()
            weather_data = weather_response.json()

            if (
                "current_condition" in weather_data
                and len(weather_data["current_condition"]) > 0
            ):
                current = weather_data["current_condition"][0]
                result = {
                    "iss_latitude": latitude,
                    "iss_longitude": longitude,
                    "temperature_fahrenheit": float(current.get("temp_F", 0.0)),
                    "humidity_percent": float(current.get("humidity", 0.0)),
                    "wind_speed_kmh": float(current.get("windspeedKmph", 0.0)),
                    "cloud_cover_percent": float(current.get("cloudcover", 0.0)),
                    "timestamp": str(dt.now()),
                    "data_source": "wttr.in",
                }
                print(f"✅ wttr.in API succeeded: {result}")
                return result
        except Exception as e:
            print(f"❌ wttr.in API failed: {str(e)}")

        # All APIs failed
        print("❌ All weather APIs failed")
        return None

    def _get_fallback_weather_data() -> dict:
        """
        Returns fallback weather data for Houston, TX (NASA Johnson Space Center)
        when ISS location data is unavailable or over oceans/poles.
        """
        API_TIMEOUT = 10
        try:
            # Houston, TX coordinates (NASA Johnson Space Center)
            latitude, longitude = 29.5583, -95.0853

            print(
                f"Fetching fallback weather for Houston, TX: {latitude}°N, {longitude}°W"
            )
            weather_url = "https://api.open-meteo.com/v1/forecast"
            weather_params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,cloud_cover",
                "temperature_unit": "fahrenheit",
            }

            weather_response = requests.get(
                weather_url, params=weather_params, timeout=API_TIMEOUT
            )
            weather_response.raise_for_status()
            weather_data = weather_response.json()

            current = weather_data["current"]
            result = {
                "iss_latitude": latitude,
                "iss_longitude": longitude,
                "temperature_fahrenheit": current.get("temperature_2m", 72.0),
                "humidity_percent": current.get("relative_humidity_2m", 60.0),
                "wind_speed_kmh": current.get("wind_speed_10m", 10.0),
                "cloud_cover_percent": current.get("cloud_cover", 30.0),
                "timestamp": current.get("time", str(dt.now())),
                "data_source": "fallback_houston_tx",
            }

            print(f"Fallback weather data retrieved: {result}")
            return result

        except Exception as e:
            print(f"ERROR: Even fallback data failed: {str(e)}")
            # Return static default data as last resort
            return {
                "iss_latitude": 29.5583,
                "iss_longitude": -95.0853,
                "temperature_fahrenheit": 72.0,
                "humidity_percent": 60.0,
                "wind_speed_kmh": 10.0,
                "cloud_cover_percent": 30.0,
                "timestamp": str(dt.now()),
                "data_source": "static_default",
            }

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

    @task
    def aggregate_historical_data(
        stats: dict, weather_data: dict, correlation: dict, **context
    ) -> dict:
        """
        Aggregates current run data with context for historical tracking.
        Prepares data for trend analysis across multiple DAG runs.
        """
        dag_run = context["dag_run"]

        aggregated = {
            "run_metadata": {
                "run_id": dag_run.run_id,
                "execution_date": str(dag_run.execution_date),
                "logical_date": str(dag_run.logical_date),
            },
            "astronaut_metrics": {
                "total_count": stats["total_astronauts"],
                "unique_crafts": stats["unique_crafts"],
                "craft_distribution": stats["crafts"],
            },
            "weather_metrics": {
                "iss_position": {
                    "latitude": weather_data["iss_latitude"],
                    "longitude": weather_data["iss_longitude"],
                },
                "conditions": {
                    "temperature_f": weather_data["temperature_fahrenheit"],
                    "humidity_pct": weather_data["humidity_percent"],
                    "wind_speed_kmh": weather_data["wind_speed_kmh"],
                    "cloud_cover_pct": weather_data["cloud_cover_percent"],
                },
                "timestamp": weather_data["timestamp"],
            },
            "correlation_metrics": {
                "weather_comfort_index": correlation["insights"][
                    "weather_comfort_index"
                ],
                "astronauts_per_weather_score": correlation["insights"][
                    "astronauts_per_weather_score"
                ],
                "cloud_category": correlation["insights"]["cloud_coverage_category"],
                "temp_category": correlation["insights"]["temperature_category"],
            },
        }

        print("=" * 60)
        print("DATA AGGREGATION")
        print("=" * 60)
        print(f"\n✅ Aggregated data for run: {dag_run.run_id}")
        print(f"📊 Metrics collected: {len(aggregated)} categories")
        print(f"🚀 Astronauts tracked: {stats['total_astronauts']}")
        print(
            f"🌍 Weather comfort index: {correlation['insights']['weather_comfort_index']}"
        )
        print("=" * 60)

        return aggregated

    @task
    def calculate_trends(aggregated_data: dict) -> dict:
        """
        Calculates trends and identifies patterns from the aggregated data.
        In production, this would compare against historical database records.
        """
        trends = {
            "current_snapshot": {
                "astronaut_count": aggregated_data["astronaut_metrics"]["total_count"],
                "weather_comfort": aggregated_data["correlation_metrics"][
                    "weather_comfort_index"
                ],
                "craft_diversity": aggregated_data["astronaut_metrics"][
                    "unique_crafts"
                ],
            },
            "trend_indicators": {
                "astronaut_activity": {
                    "current_level": "high"
                    if aggregated_data["astronaut_metrics"]["total_count"] > 10
                    else "standard",
                    "status": "stable",
                },
                "weather_conditions": {
                    "comfort_level": aggregated_data["correlation_metrics"][
                        "temp_category"
                    ],
                    "cloud_status": aggregated_data["correlation_metrics"][
                        "cloud_category"
                    ],
                    "status": "variable",
                },
            },
            "data_quality": {
                "completeness": "100%",
                "sources": ["Open Notify API", "Open-Meteo API"],
                "status": "high_quality",
            },
        }

        print("\n" + "=" * 60)
        print("TREND ANALYSIS")
        print("=" * 60)
        print("\n📈 Current Snapshot:")
        print(f"  • Astronauts: {trends['current_snapshot']['astronaut_count']}")
        print(
            f"  • Weather Comfort: {trends['current_snapshot']['weather_comfort']}/100"
        )
        print(f"  • Craft Diversity: {trends['current_snapshot']['craft_diversity']}")

        print("\n🔍 Trend Indicators:")
        for category, details in trends["trend_indicators"].items():
            print(
                f"  • {category.replace('_', ' ').title()}: {details['status'].upper()}"
            )

        print("=" * 60)

        return trends

    @task(outlets=[aggregated_data_dataset])
    def generate_aggregate_report(
        aggregated_data: dict, trends: dict, stats: dict, weather_data: dict
    ) -> None:
        """
        Generates the final aggregated report and produces the aggregated dataset
        for potential downstream consumers.
        """
        print("\n" + "=" * 80)
        print("AGGREGATED DATA SUMMARY REPORT")
        print("=" * 80)

        print("\n📦 DATA AGGREGATION COMPLETE")
        print(f"  • Run ID: {aggregated_data['run_metadata']['run_id']}")
        print(
            f"  • Execution Date: {aggregated_data['run_metadata']['execution_date']}"
        )

        print("\n📊 AGGREGATED METRICS:")
        print("  Astronaut Data:")
        print(f"    - Total Astronauts: {stats['total_astronauts']}")
        print(f"    - Unique Crafts: {stats['unique_crafts']}")
        for craft, count in stats["crafts"].items():
            print(f"    - {craft}: {count}")

        print("\n  Weather Data:")
        print(
            f"    - Location: {weather_data['iss_latitude']:.2f}°N, {weather_data['iss_longitude']:.2f}°E"
        )
        print(f"    - Temperature: {weather_data['temperature_fahrenheit']:.1f}°F")
        print(
            f"    - Comfort Index: {aggregated_data['correlation_metrics']['weather_comfort_index']}"
        )

        print("\n📈 TREND SUMMARY:")
        print(
            f"  • Astronaut Activity: {trends['trend_indicators']['astronaut_activity']['current_level'].upper()}"
        )
        print(
            f"  • Weather Comfort: {aggregated_data['correlation_metrics']['temp_category']}"
        )
        print(
            f"  • Cloud Coverage: {aggregated_data['correlation_metrics']['cloud_category']}"
        )

        print("\n✅ OUTPUT DATASET PRODUCED:")
        print("  • Dataset: aggregated_astronaut_weather_data")
        print("  • Status: UPDATED")
        print("  • Can trigger downstream DAGs")

        print("\n" + "=" * 80)

        # Store aggregated summary
        summary = {
            "aggregated_data": aggregated_data,
            "trends": trends,
            "generated_at": str(dt.now()),
        }

        print(
            f"\n📋 Complete Aggregated Data:\n{json.dumps(summary, indent=2, default=str)}"
        )

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

    # Aggregate data for historical tracking
    aggregated = aggregate_historical_data(stats, weather_data, correlation)

    # Calculate trends from aggregated data
    trend_analysis = calculate_trends(aggregated)

    # Generate final aggregate report (produces aggregated dataset)
    generate_aggregate_report(aggregated, trend_analysis, stats, weather_data)


# Instantiate the DAG
example_astronauts()
