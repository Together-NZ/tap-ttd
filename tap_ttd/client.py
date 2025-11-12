"""REST client handling, including ttdStream base class."""

from __future__ import annotations
import csv
import decimal
import typing as t
from importlib import resources
from typing import Any, Dict, Iterable, Optional
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from datetime import datetime
import json 
import time
import io
import jsonschema
from jsonschema import validate
from googleapiclient.http import MediaIoBaseDownload
import datetime
import requests
from singer_sdk.helpers.types import Context
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Add a console handler to see logs in the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)


class ttdStream(RESTStream):
    """ttd stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105
    replication_key = "Date"
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.thetradedesk.com"

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="x-api-key",
            value=self.config.get("api_key", ""),
            location="header",
        )
    def get_url(self, context: Optional[Dict[str, Any]]) -> str:
        """Construct the URL for the current operation."""
        operation = context.get("operation")

        if operation == "query":
            return f"{self.url_base}/v3/myreports/reportschedule"
        elif operation == "run":
            return f"{self.url_base}/v3/myreports/reportexecution/query/advertisers"
        else:
            raise ValueError(f"Unknown operation: {operation}")


    def request_records(self, context: Optional[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        if context is None:
            context = {}

        # Default operation to 'query' if not set
        context.setdefault("operation", "query")

        # Step 1: Submit Query
        if context["operation"] == "query":
            url = self.get_url(context)
            if self.config["manual"]:
                if self.config["end_date"] == "{END_DATE}T00:00:00Z" or self.config["end_date"] is None:
                    # Default to today's date in ISO format
                    
                    end_date = datetime.datetime.now().strftime("%Y-%m-%dT00:00:00Z")
                else:
                    end_date = self.config["end_date"]   # Use the provided 'end_date'
                payload = {
                "ReportDateFormat": "International",
                "ReportDateRange": "Custom",
                "ReportStartDateInclusive": self.config["start_date"],
                "ReportEndDateExclusive": end_date,
                "ReportFileFormat": "CSV",
                "ReportFrequency": "Once",
                "IncludeHeaders": "true",
                "ReportNumericFormat": "International",
                "ReportScheduleName": "TTD",
                "ReportTemplateId": 168005,
                "TimeZone": "Pacific/Auckland",
                "AdvertiserFilters": [self.config["advertiser_id"]],
                }
                logger.info('Querying TTD Report with Manual Mode')
            else:
                payload = {
                "ReportDateFormat": "International",
                "ReportDateRange": "LastXDays",
                "LookbackDays":1,
                "ReportFileFormat": "CSV",
                "ReportFrequency": "Once",
                "IncludeHeaders": "true",
                "ReportNumericFormat": "International",
                "ReportScheduleName": "TTD",
                "ReportTemplateId": 168005,
                "TimeZone": "Pacific/Auckland",
                "AdvertiserFilters": [
                self.config["advertiser_id"]
                ],
                }
                logger.info('Querying TTD Report with Automatic Mode')

            headers = {"TTD-Auth": self.config["api_key"]}
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code != 200:
                raise RuntimeError(f"API call failed: {response.status_code} - {response.text}")

            response_data = response.json()
            report_schedule_id = response_data["ReportScheduleId"]
            context["operation"] = "run"
            context["report_schedule_id"] = report_schedule_id

        # Step 2: Run the Query to Generate a Report
        if context["operation"] == "run":
            report_schedule_id = context["report_schedule_id"]
            url = self.get_url(context)
            headers = {"TTD-Auth": self.config["api_key"]}
            payload = {
                "AdvertiserIds": [self.config["advertiser_id"]],
                "PageSize": 10,
                "PageStartIndex": 0,
                "ReportScheduleIds": [report_schedule_id],
                "ReportScheduleNameContains": "TTD",
            }
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code != 200:
                raise RuntimeError(f"Failed to run query: {response.status_code} - {response.text}")

            response_data = response.json()
            initial_delay = 20
            max_retries = 10
            retry_count = 0
            time.sleep(100)
            while retry_count < max_retries:
                state = response_data["Result"][0]["ReportExecutionState"]
                if state == "Complete":
                    logger.info(f"Report execution state: {state}")
                    context["operation"] = "download"
                    context["response_data"] = response_data
                    break
                elif state != "Complete":
                    logger.info(f"Report execution state: {state}. Retrying in {initial_delay} seconds...")
                    time.sleep(initial_delay)
                    retry_count += 1
                    initial_delay *= 2
                    response = requests.post(url, headers=headers, json=payload)
                    if response.status_code == 200:
                        response_data = response.json()
                    else:
                        raise RuntimeError(f"Retry failed: {response.status_code} - {response.text}")
            else:
                raise RuntimeError("Maximum retries reached. Report execution failed.")

        # Step 3: Poll for Report Status
        if context["operation"] == "download":
            response_data = context["response_data"]
            download_url = response_data["Result"][0]["ReportDeliveries"][0].get("DownloadURL")
            headers = {"TTD-Auth": self.config["api_key"]}
            response = requests.get(download_url, headers=headers, stream=True)
            
            if response.status_code == 200:
                #output_path= 'report.csv'
                #with open(output_path, 'wb') as f:
                #    for chunk in response.iter_content(chunk_size=1024):
                #        if chunk:
                #            f.write(chunk)
                buf = io.BytesIO()
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        buf.write(chunk)
                buf.seek(0)
            else:
                raise RuntimeError(f"Failed to download the file: {response.status_code} - {response.text}")

            context["operation"] = "retrieve"
            context["file_buffer"] = buf

        # Step 4: Retrieve and Process Report
        if context["operation"] == "retrieve":
            buf = context["file_buffer"]
            csv_reader = csv.reader(io.TextIOWrapper(buf, "utf-8"))
            start_reading = False
            #with open(file_path, 'w', newline='', encoding='utf-8') as f:
            #    writer = csv.writer(f)  # Create a CSV writer object
            #    for row in csv_reader:
            #        writer.writerow(row)  # Use writer.writerow() to write each row
            for row in csv_reader: 
                logger.info('testing')
                if not start_reading:
                    logger.info("Can not start reading now")
                    if 'Date' in row:
                        logger.info('Found Date')
                        start_reading = True
                        continue
                else:
                        logger.info(f"Processing row: {row}")                
                        schema_fields = list(self.schema["properties"].keys())
                        # Ensure the row has the correct number of fields
                        if len(row) != len(schema_fields):
                            logger.info(f"Skipping row with incorrect number of fields: {row}")
                            logger.info(len(row),len(schema_fields))
                            continue

                        # Map the row values to schema fields
                        row_dict = {}
                        for i, value in enumerate(row):
                            try:
                                if schema_fields[i] in ["clicks", "impressions"]:
                                    row_dict[schema_fields[i]] = int(value)
                                else:
                                    row_dict[schema_fields[i]] = value
                            except ValueError:
                                row_dict[schema_fields[i]] = None  # Handle invalid numbers

                        try:
                            # Validate the mapped row against the schema
                            logger.info(f"Validating row: {row_dict}")
                            validate(instance=row_dict, schema=self.schema)
                            yield row_dict  # Yield the validated row as a dictionary
                        except jsonschema.exceptions.ValidationError as e:
                            # Handle validation errors (e.g., log or skip invalid rows)
                            self.logger.error(f"Validation error in row {row}: {e}")



