import base64
import importlib
import json
import logging
import os
import pathlib

import yaml
import zoo
import zoo_wes_runner
from zoo_wes_runner import ZooWESRunner

import sys
import traceback
import boto3  # noqa: F401
import botocore
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from botocore.client import Config
from pystac import read_file
from pystac.stac_io import DefaultStacIO, StacIO
from pystac.item_collection import ItemCollection
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()
        self.s3_client = self.session.create_client(
            service_name="s3",
            region_name=os.environ.get("AWS_REGION"),
            endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            verify=True,
            use_ssl=True,
            config=Config(s3={"addressing_style": "path", "signature_version": "s3v4"}),
        )

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            return (
                self.s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=parsed.netloc,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        else:
            super().write_text(dest, txt, *args, **kwargs)


StacIO.set_default(CustomStacIO)


class WESRunnerExecutionHandler:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.job_id = None

    def post_execution_hook(self, log, output, usage_report, tool_logs):

        myKey=list(output.keys())[0]
        # unset HTTP proxy or else the S3 client will use it and fail
        os.environ.pop("HTTP_PROXY", None)

        os.environ["AWS_S3_REGION"] = self.get_additional_parameters()["STAGEOUT_AWS_REGION"]
        os.environ["AWS_S3_ENDPOINT"] = self.get_additional_parameters()["STAGEOUT_AWS_SERVICEURL"]
        os.environ["AWS_ACCESS_KEY_ID"] = self.get_additional_parameters()["STAGEOUT_AWS_ACCESS_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.get_additional_parameters()["STAGEOUT_AWS_SECRET_ACCESS_KEY"]

        zoo.info("Post execution hook")

        StacIO.set_default(CustomStacIO)

        zoo.info(f"Read catalog from STAC Catalog URI: {output[myKey]}")

        cat = read_file(output[myKey])

        collection_id = self.get_additional_parameters()["sub_path"]

        zoo.info(f"Create collection with ID {collection_id}")

        collection = None

        collection = next(cat.get_all_collections())

        zoo.info("Got collection {collection.id} from processing outputs")

        items = []

        for item in collection.get_all_items():

            zoo.info("Processing item {item.id}")

            for asset_key in item.assets.keys():

                zoo.info(f"Processing asset {asset_key}")

                temp_asset = item.assets[asset_key].to_dict()
                temp_asset["storage:platform"] = "eoap"
                temp_asset["storage:requester_pays"] = False
                temp_asset["storage:tier"] = "Standard"
                temp_asset["storage:region"] = self.get_additional_parameters()[
                    "STAGEOUT_AWS_REGION"
                ]
                temp_asset["storage:endpoint"] = self.get_additional_parameters()[
                    "STAGEOUT_AWS_SERVICEURL"
                ]
                item.assets[asset_key] = item.assets[asset_key].from_dict(temp_asset)
            item.collection_id = collection_id

            items.append(item.clone())

        item_collection = ItemCollection(items=items)

        zoo.info("Created feature collection from items")

        # Trap the case of no output collection
        if item_collection is None:
            zoo.error("The output collection is empty")
            self.feature_collection = json.dumps({}, indent=2)
            return

        # Set the feature collection to be returned
        self.results = item_collection.to_dict()
        self.results["id"] = collection_id

    def local_get_file(self, fileName):
        """
        Read and load a yaml file

        :param fileName the yaml file to load
        """
        try:
            with open(fileName, 'r') as file:
                additional_params = yaml.safe_load(file)
            return additional_params
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}
        except Exception():
            return {}

    def set_job_id(self, job_id):
        self.job_id = job_id

    def get_pod_env_vars(self):
        zoo.info("get_pod_env_vars")

        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):
        zoo.info("get_pod_node_selector")

        return self.conf.get("pod_node_selector", {})

    def get_secrets(self):
        zoo.info("get_secrets")

        return self.local_get_file("/assets/pod_imagePullSecrets.yaml")

    def get_additional_parameters(self):
        zoo.info("get_additional_parameters")
        additional_parameters: Dict[str, str] = {}
        additional_parameters = self.conf.get("additional_parameters", {})

        additional_parameters["sub_path"] = self.conf["lenv"]["usid"]

        zoo.info(f"additional_parameters: {additional_parameters.keys()}")

        return additional_parameters

    def handle_outputs(self, log, output, usage_report, tool_logs):
        os.makedirs(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id),
            mode=0o777,
            exist_ok=True,
        )
        with open(os.path.join(self.conf["main"]["tmpPath"], self.job_id, "job.log"), "w") as f:
            f.writelines(log)

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "output.json"), "w"
        ) as output_file:
            json.dump(output, output_file, indent=4)

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "usage-report.json"),
            "w",
        ) as usage_report_file:
            json.dump(usage_report, usage_report_file, indent=4)

        aggregated_outputs = {}
        aggregated_outputs = {
            "usage_report": usage_report,
            "outputs": output,
            "log": os.path.join(self.job_id, "job.log"),
        }

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "report.json"), "w"
        ) as report_file:
            json.dump(aggregated_outputs, report_file, indent=4)


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):

    with open(
        os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        ),
        "r",
    ) as stream:
        cwl = yaml.safe_load(stream)

    execution_handler=WESRunnerExecutionHandler(conf=conf)
    runner = ZooWESRunner(
        cwl=cwl,
        conf=conf,
        inputs=inputs,
        outputs=outputs,
        execution_handler=execution_handler,
    )
    exit_status = runner.execute()

    # Fetch the logs whatever the exit status is
    if runner is not None and runner.run_log_content is not None:
        with open(os.path.join(
                    conf["main"]["tmpPath"],
                    f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}_job.log"
                ),"w+") as f:
            f.write(runner.run_log_content)
        conf["service_logs"]={
            "url": os.path.join(
                conf["main"]["tmpUrl"].replace("/temp","/"+conf["auth_env"]["user"]+"/temp"),
                f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}_job.log"
            ),
            "title": f"TOIL run log",
            "rel": "related",
        }
    if exit_status == zoo.SERVICE_SUCCEEDED:
        json_out_string= json.dumps(runner.demo_outputs[list(runner.demo_outputs.keys())[0]], indent=4)
        outputs[list(outputs.keys())[0]]["value"] = json.dumps(
            execution_handler.results, indent=2
        )
        return zoo.SERVICE_SUCCEEDED
    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
