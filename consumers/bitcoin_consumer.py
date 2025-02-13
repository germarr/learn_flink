from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    TableDescriptor,
    Schema,
    DataTypes,
    FormatDescriptor
)

from pyflink.table import expressions as expr
from pyflink.table.expressions import col

dockerC = "kafka-broker:9092"

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)    # Point to the SQL Kafka connector JAR
# kafka_connector_jar = "file:///app/flink-sql-connector-kafka-1.17.2.jar"

# t_env.get_config().set("pipeline.jars", kafka_connector_jar)

# Define the Kafka source table
t_env.create_temporary_table(
    'bitcoin_table4',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('name_coin', DataTypes.STRING())
                .column('timestamp', DataTypes.TIMESTAMP(3))
                .column('open', DataTypes.FLOAT())
                .column('high', DataTypes.FLOAT())
                .column('low', DataTypes.FLOAT())
                .column('close', DataTypes.FLOAT()) 
                .column('volume', DataTypes.FLOAT())  
                .column('current_time', DataTypes.TIMESTAMP(3))
                .column_by_expression('start_of_hour', expr.call_sql("CAST(FLOOR(`timestamp` TO HOUR) AS TIMESTAMP(3))"))               
                .build())
        .option('topic', 'bitcoin_price_2')
        .option('properties.bootstrap.servers', dockerC)
        .option('properties.group.id', 'transaction_group')
        .option('scan.startup.mode', 'earliest-offset')
        .format(FormatDescriptor.for_format('json')
                .option('fail-on-missing-field', 'false')
                .option('ignore-parse-errors', 'true')
                .build())
        .build())

# Define the Kafka Sink Table
t_env.create_temporary_table(
        'bitcoin_sink',
        TableDescriptor.for_connector('kafka')
                .schema(Schema.new_builder()
                        .column('name_coin', DataTypes.STRING())
                        .column('open', DataTypes.FLOAT())
                        .build())
                .option('topic', 'bitcoin_summary')
                .option('properties.bootstrap.servers', dockerC)
                .format(FormatDescriptor.for_format('json')
                        .build())
                .build())

transactions = t_env.from_path("bitcoin_table4")

# sales_report = transactions.drop_columns(col('timestamp'), col('high'), col('low'), col('close'), col('volume'), col('current_time'), col('start_of_hour')) 

sales_report = transactions.select(col('name_coin'), col('open'))

# transactions.execute().print()

sales_report.execute_insert("bitcoin_sink").wait()