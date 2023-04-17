# GrowSmart
* * *
GrowSmart helps you live a healthier life through your garden. 
This tool is designed to help us process, analyze and make sense of our data.

## Prerequisites

To use GrowSmart, you will need to have the following installed on your computer:

- Docker
- Docker Compose

It is highly recommended that you have at least 8GB of RAM in your machine.

## Installation

To install and set up GrowSmart, follow these steps:

1. Clone the repository to your local machine: `git clone https://github.com/Action52/GrowSmart.git`
2. Navigate to the `airflow` directory: `cd growsmart/airflow`
3. Run the following command to start the Docker containers: `docker-compose up -d`
4. Wait for the containers to start up (this may take a few minutes).
5. Access the Airflow web UI by opening your browser and navigating to `http://localhost:8080`

<i>Note: If it's your first time running the airflow container you will have to set up the s3 connection variables. To do so:</i>

1. Once the containers are up, navigate to the UI `http://localhost:8080`. Log in with the default user (airflow) and password (same as user).
2. Go to Admin -> Variables. Click "+".
3. Add key: "aws_access_key", with the corresponding value.
4. Click Save.
5. Repeat process for key: "aws_secret_access_key".

AWS Key Data is included on the report as an annex.

## Usage

Once you have the Docker containers running, you can use GrowSmart through Airflow to process your data. Follow these steps:

1. Ensure you have credentials to access our data buckets. (Annexed on the project report).
2. Activate the DAG you want to use (AirflowAWSWeather, AirflowAWSWeather, iot_devices_data, iot_device_persistent_data, plant_traits_data_collector, plant_traits_persistent_data_collector).
3. Go to the right and click "run DAG".

## Contributing

If you would like to contribute to GrowSmart, feel free to submit a pull request. We welcome contributions from the community!

## License

GrowSmart is released under the MIT license. See `LICENSE` for more information.