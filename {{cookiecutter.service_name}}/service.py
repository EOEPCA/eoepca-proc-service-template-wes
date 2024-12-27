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


class WESRunnerExecutionHandler:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.job_id = None

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

    def get_additional_parameters(self):
        # sets the additional parameters for the execution
        # of the wrapped Application Package

        zoo.info("get_additional_parameters")

        additional_parameters = {
            "s3_bucket": "results",
            "sub_path": self.conf["lenv"]["usid"],
            "region_name": "us-east-1",
            "aws_secret_access_key": "test",
            "aws_access_key_id": "test",
            "endpoint_url": "http://localhost:4900",
        }

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

    runner = ZooWESRunner(
        cwl=cwl,
        conf=conf,
        inputs=inputs,
        outputs=outputs,
        execution_handler=WESRunnerExecutionHandler(conf=conf),
    )
    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        json_out_string= json.dumps(runner.demo_outputs["s3_catalog_output"], indent=4)
        outputs[list(outputs.keys())[0]]["value"]=json_out_string
        with open(os.path.join(
                    conf["main"]["tmpPath"],
                    f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}.log"
                ),"w+") as f:
            f.write(runner.run_log_content)
        conf["service_logs"]={
            "url": os.path.join(
                conf["main"]["tmpUrl"],
                f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}.log"
            ),
            "title": f"TOIL run log",
            "rel": "related",
        }
        return zoo.SERVICE_SUCCEEDED
    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
