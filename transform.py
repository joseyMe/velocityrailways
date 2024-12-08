import pandas as pd
import requests
import json
import time
from datetime import datetime
import pickle
import psycopg2     
from psycopg2 import sql
from dotenv import load_dotenv 
import os
import logging
import great_expectations as ge
import great_expectations as ge
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import get_context
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import get_context
import tempfile

# Transformation

def transformation_layer(**kwargs):

    ti = kwargs['ti']
    response =ti.xcom_pull(key="raw_data", task_ids="extract")


    train_columns = [{'request_time': response['request_time'], 'station_name': response['station_name']}]
    train_columns_df = pd.DataFrame(train_columns).reset_index()

    train__departure_columns = []

    for columns in response['departures']['all']:
        try:
            row ={
                'mode':columns['mode'],
                'train_uid': columns['train_uid'],
                'origin_name': columns['origin_name'],
                'operator_name':columns['operator_name'],
                'platform': columns['platform'],
                'destination_name': columns['destination_name'],
                'aimed_departure_time': columns['aimed_departure_time'],
                'expected_departure_time': columns['expected_departure_time'],
                'best_departure_estimate_mins': columns['best_departure_estimate_mins'],
                'aimed_arrival_time': columns['aimed_arrival_time']
            }
            train__departure_columns.append( row)
        except (KeyError, TypeError) as e:
            print(f"Error processing columns: {e}")     

    print(train__departure_columns)




    train__departure_columns_df = pd.DataFrame(train__departure_columns)
    train__departure_columns_df.reset_index(inplace=True)
    train__departure_columns_df

    #Merging both dataframes

    raw_train_schedule_df = pd.merge(train_columns_df, train__departure_columns_df, on='index', how='outer' )
    raw_train_schedule_df

    #now lets rename some of the columns
    train_schedule_df = raw_train_schedule_df

    #Renaming request time column
    train_schedule_df.rename(columns= {'request_time':'request_date_time'}, inplace = True)
    train_schedule_df['request_date_time'] = train_schedule_df['request_date_time'].ffill()
    train_schedule_df['station_name'] = train_schedule_df['station_name'].ffill()

    # Fill all Null values 
    train_schedule_df.fillna('unknown', inplace=True)

    #Dropping the index column
    train_schedule_df.drop( axis=0, columns='index', level=None, inplace=True, errors='raise')


    #validating transformed data

    try:
        context = get_context()


        suite = ExpectationSuite("train_schedule_suite")


        # Set up an execution engine
        execution_engine = PandasExecutionEngine()

        # Use a Batch to wrap the DataFrame
        batch = Batch(data=train_schedule_df)

        # Step 3: Create an Expectation Suite
        #suite_name = "train_schedule_suite"
        #suite = ExpectationSuite(expectation_suite_name=suite_name)

        # Create a Validator with the Batch and ExpectationSuite
        validator = Validator(
            execution_engine=execution_engine,
            batches=[batch],
            expectation_suite=suite
        )

        # Add expectations directly to the Validator
        validator.expect_column_values_to_not_be_null(column="request_date_time")
        validator.expect_column_values_to_not_be_null(column="station_name")
        validator.expect_column_values_to_not_be_null(column="mode")
        validator.expect_column_values_to_not_be_null(column="train_uid")
        validator.expect_column_values_to_not_be_null(column="origin_name")
        validator.expect_column_values_to_not_be_null(column="operator_name")
        validator.expect_column_values_to_not_be_null(column="platform")
        validator.expect_column_values_to_not_be_null(column="destination_name")
        validator.expect_column_values_to_not_be_null(column="aimed_departure_time")
        validator.expect_column_values_to_not_be_null(column="expected_departure_time")
        validator.expect_column_values_to_not_be_null(column="best_departure_estimate_mins")
        validator.expect_column_values_to_not_be_null(column="aimed_arrival_time") 

        # Step 6: Validate the DataFrame and print the results
        transformed_results = validator.validate()
        print("Validation results:", transformed_results)

    except Exception as e:
        print(f"Data Validation failed: {e}")
        
    ti.xcom_push(key="transformed_data", value=transformed_results)
    return transformed_results

if __name__ == "__main__":
    transformation_layer()