# Databricks notebook source
from dbacademy import dbgems

class PipelineConfig:
    """
    Represents pipeline settings with option to provide as JSON definition.

      Attributes:
          Configurable: name, storage, target, configuration, notebooks, policy
          Auto-generated: libraries (from notebooks), clusters (from policy)
          Non-configurable: continuous (False), development (True), photon (False)

      Methods:
          get_pipeline_settings(convert_to_json=False): 
              Returns pipeline settings as dictonary or JSON string

          print_pipeline_config(): 
              Displays parameters as text and input textboxes

    """

    def __init__(self, name, notebooks, configuration, storage, target, policy=None, photon=False):
        """
        Defines PipelineConfig attributes.    

            Use input values to set attributes: name, storage, target

            Modify input values to set attributes:
                configuration: add "spark.master": "local[*]" to configuration
                notebooks: convert to list of notebook paths
                libraries: create list of dictionaries from notebook paths
                clusters: create based on policy
            
            Use default values to set attributes:
                continuous: False
                development: True
                photon: False
        """
        self.name = name
        self.storage = storage
        self.target = target
        self.configuration = {**configuration, "spark.master": "local[*]"}

        basepath = dbgems.get_notebook_dir()

        self.notebooks = [f"{basepath}/{n}" for n in notebooks]
        self.libraries = [{"notebook": {"path": n}} for n in self.notebooks]
        self.policy = policy
    
        if policy:
            clusters = [{"num_workers": 0, "policy_id": policy.get("policy_id")}]
        else:
            clusters = [{"num_workers": 0}]
        self.clusters = clusters
            
        self.continuous = False
        self.development = True
        self.photon = photon


    def get_pipeline_settings(self, convert_to_json=False):
        """
        Returns pipeline settings as dictonary or JSON string.

        :param convert_to_json: if True, return JSON string; if False (default), return dictionary
        :return: pipeline settings as a dictionary or JSON string, based on convert_to_json
        """
        params = dict()
        params["name"] = self.name
        params["target"] = self.target
        params["storage"] = self.storage
        params["libraries"] = self.libraries    
        params["configuration"] = self.configuration
        params["clusters"] = self.clusters        
        params["continuous"] = self.continuous
        params["development"] = self.development
        params["photon"] = self.photon

        if not convert_to_json:
            return params
        else:
            import json
            return json.dumps(self.get_pipeline_settings())


    def get_config_values(self):
        """
        Returns a subset of pipeline parameters as a list of (name, value) tuples.
        This is meant to be displayed using DBAcademyHelper.display_config_values to provide instructions.

        Included settings:
            Pipeline Name: <name>
            Storage Location: <path>
            Target: <name>
            Policy: <name>

            All configuration properties, excluding "spark.master"; usually includes "source"
                source: <path>

            All notebook paths, numbered starting from 1:
                Notebook #1 Path: <path>
                Notebook #2 Path: <path>

        See also DBAcademyHelper.display_config_values
        """

        config_values = [
            ("Pipeline Name", self.name),
            ("Storage Location", self.storage),
            ("Target", self.target),            
            ("Policy", "None" if self.policy is None else self.policy.get("name"))
        ]
        for key, val in self.configuration.items():
            if key != "spark.master": config_values.append((key, val))
            
        for i, path in enumerate(self.notebooks):
            config_values.append((f"Notebook #{i+1} Path", path))
            
        return config_values

None        

# COMMAND ----------

# Define helper functions to configure, create, and trigger pipelines with DBAcademyHelper

@DBAcademyHelper.monkey_patch
def configure_pipeline(self, notebooks, name=None, source=None, configuration=None, photon=False):
    """
    Creates PipelineConfig object for provided notebooks and optional settings.
    Defines parameter values using DBAcademyHelper:
        Attributes:
            target
            paths.storage
            pipeline_name (if name not provided)
            source (if source not provided)
        Methods:
            get_dlt_policy()
    :param notebooks (list): list of notebook paths, relative to current notebook
    :param source (str): value for "source" configuration property
    :param configuration (dict): overrides source to include more than one "source" configuration property (optional)
    :param name (str): overrides self.pipeline_name to create pipeline name
    """
    
    notebooks = [f"Pipeline/{f}" for f in notebooks]
    
    if name is None:
        name = self.pipeline_name

    if configuration:
        assert source is None, "source is ignored when configuration is provided"
    elif source:
        configuration = {"source": source}
    else:
        configuration = {"source": self.paths.stream_source}
    

    self.pipeline_config = PipelineConfig(
        name=name,
        notebooks=notebooks,        
        configuration=configuration,
        storage=self.paths.storage_location,
        target=self.schema_name,
        policy=self.get_dlt_policy(),
        photon=photon
    )

@DBAcademyHelper.monkey_patch
def get_dlt_policy(self):
    """
    Returns cluster policy for DLT pipelines, created by Workspace-Setup script.
    Get cluster policy using name provided by DBAcademy ClustersHelper.POLICY_DLT_ONLY
    """
    from dbacademy import common
    from dbacademy.dbhelper import ClustersHelper
    dlt_policy = self.client.cluster_policies.get_by_name(ClustersHelper.POLICY_DLT_ONLY)
    if dlt_policy is None: 
        common.print_warning("WARNING: Policy Not Found", 
        f"Could not find the cluster policy \"{ClustersHelper.POLICY_DLT_ONLY}\".\nPlease run the notebook Includes/Workspace-Setup to address this error.")
    return dlt_policy


@DBAcademyHelper.monkey_patch
def generate_pipeline(self, config=None, show_validate=False):
    """
    Creates pipeline using provided PipelineConfig or DBAcademyHelper.pipeline_config.

    See also DBAcademyHelper.create_pipeline
    See also DBAcademyHelper.validate_pipeline_config
    See also PipelineConfig.print_pipeline_config

    :param config: overrides DBAcademyHelper.pipeline_config (optional)
    :param show_validate: if False (default), hide validation results
    :return: pipeline ID
    """
    if config is None: config = self.pipeline_config
     
    self.display_config_values(config.get_config_values())

    self.pipeline_id = self.create_pipeline(config)

    if show_validate: self.validate_pipeline_config(config, display=False)    

    return self.pipeline_id



@DBAcademyHelper.monkey_patch
def create_pipeline(self, config=None):
    """
    Create or replace pipeline with the provided configuration, then display link to Pipeline UI.
    See also DBAcademyHelper.create_pipeline_from_settings.

    Sets DBAcademyHelper attribute:
        pipeline_id: Pipeline ID of the created pipeline

    :param config: PipelineConfig to override self.pipeline_config (optional)
    :return: the pipeline ID of the created pipeline
    """
    if not config: 
        config = self.pipeline_config
    settings = config.get_pipeline_settings()

    self.pipeline_id = self.create_pipeline_from_settings(settings)
    
    pipeline_url = f"{dbgems.get_workspace_url()}#joblist/pipelines/{self.pipeline_id}"
    displayHTML(f"""Created the pipeline "{settings["name"]}": <a href={pipeline_url}>{self.pipeline_id}</a>""")

    return self.pipeline_id
        

@DBAcademyHelper.monkey_patch
def create_pipeline_from_settings(self, settings):
    """
    Create or replace pipeline with the provided configuration, then return pipeline ID.
    See also DBAcademyHelper.client.pipelines.

    :param settings: dictionary of pipeline settings
    :return: pipeline ID of the created pipeline
    """
    self.client.pipelines().delete_by_name(settings["name"]) 
    response = self.client.pipelines().create(**settings)

    return response.get("pipeline_id")


@DBAcademyHelper.monkey_patch
def start_pipeline(self, pipeline_id=None, blocking=True):
    """
    Starts the pipeline and then blocks until it has completed, failed or was canceled
    :param pipeline_id: overrides self.pipeline_id to identify pipeline (optional)
    """
    import time
    from dbacademy.dbrest import DBAcademyRestClient

    if not pipeline_id: pipeline_id = self.pipeline_id
    
    client = DBAcademyRestClient() 
    start = client.pipelines().start_by_id(pipeline_id)  # start pipeline
    update_id = start.get("update_id")

    # get status and block until done
    update = client.pipelines().get_update_by_id(pipeline_id, update_id)
    state = update.get("update").get("state")

    if blocking:
      while state not in ["COMPLETED", "FAILED", "CANCELED"]:
          duration = 15
          time.sleep(duration)
          print(f"Current state is {state}, sleeping {duration} seconds.")    
          update = client.pipelines().get_update_by_id(pipeline_id, update_id)
          state = update.get("update").get("state")

      print(f"The final state is {state}.")

      assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"

    else:
      print(f"The current state is {state}.")
      
    return update_id


@DBAcademyHelper.monkey_patch
def validate_pipeline_config(self, config, display=True):
    """
    Validate the configuration of the pipeline.
    
    - validate pipeline with name exists
    - validate settings for storage, target
    - validate libraries has correct count, includes each notebook
    - validate configuration parameters for source, spark.master
    - validate cluster settings: cluster count, autoscaling disabled, cluster policy, worker count
    - validate settings for development mode, current channel, pipeline triggered mode

    :param config: PipelineConfig to identify and validate pipeline
    :param display: if True, displays validation results in a table
    """
    from dbacademy import common
    from dbacademy.dbhelper import ClustersHelper
    suite = self.tests.new("Pipeline Config")

    # validate pipeline with name exists        
    pipeline = self.client.pipelines().get_by_name(config.name)        
    suite.test_not_none(lambda: pipeline, 
                        description=f"Create the pipeline \"<b>{config.name}</b>\".", 
                        hint="Double check the spelling.")
    if pipeline is None: pipeline = {}
    spec = pipeline.get("spec", {})

    # validate storage location and target
    suite.test_equals(lambda: spec.get("storage", None), config.storage, 
                      description=f"Set the storage location to \"<b>{config.storage}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    suite.test_equals(lambda: spec.get("target", None), config.target, 
                      description=f"Set the target to \"<b>{config.target}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")

    # validate notebooks
    libraries = [l.get("notebook", {}).get("path") for l in spec.get("libraries", [])]
    config_libraries = [l.get("notebook", {}).get("path") for l in config.libraries]
    def test_notebooks():
        if libraries is None: return False
        if len(libraries) != len(config.notebooks): return False
        for library in libraries:
            if library not in config_libraries: return False
        return True
    hint = f"""Found the following {len(libraries)} notebook(s):<ul style="margin-top:0">"""
    for library in libraries: hint += f"""<li>{library}</li>"""
    hint += "</ul>"
    suite.test(test_function=test_notebooks, actual_value=libraries, description="Configure the Notebook library.", hint=hint)

    # validate configuration parameters: source, spark.master
    suite.test_equals(lambda: spec.get("configuration", {}).get("source"), config.configuration.get("source"), 
                      description=f"Set the \"<b>source</b>\" configuration parameter to \"<b>{config.configuration.get('source')}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    suite.test_equals(lambda: spec.get("configuration", {}).get("spark.master"), "local[*]", 
                      description=f"Set the \"<b>spark.master</b>\" configuration parameter to \"<b>local[*]</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    # suite.test_length(lambda: spec.get("configuration", {}), 2, description=f"Set the two configuration parameters.", hint=f"Found [[LEN_ACTUAL_VALUE]] configuration parameter(s).")                      

    # validate cluster settings: cluster count, autoscaling disabled, cluster policy, worker count
    suite.test_length(lambda: spec.get("clusters"), expected_length=1, 
                      description=f"Expected one and only one cluster definition.", 
                      hint="Edit the config via the JSON interface to remove the second+ cluster definitions")
    suite.test_is_none(lambda: spec.get("clusters")[0].get("autoscale"), description=f"Autoscaling should be disabled.")

    def test_cluster_policy():
        cluster = spec.get("clusters")[0]
        policy_id = cluster.get("policy_id")
        if policy_id is None: common.print_warning("WARNING: Policy Not Set", 
                                                   f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\".")
        else:
            policy_name = self.client.cluster_policies.get_by_id(policy_id).get("name")
            if policy_id != self.get_dlt_policy().get("policy_id"):
                common.print_warning("WARNING: Incorrect Policy", 
                                     f"Expected the policy to be set to \"{ClustersHelper.POLICY_DLT_ONLY}\", found \"{policy_name}\".")
        return True

    suite.test(test_function=test_cluster_policy, actual_value=None, 
               description=f"The cluster policy should be <b>\"{ClustersHelper.POLICY_DLT_ONLY}\"</b>.")
    suite.test_equals(lambda: spec.get("clusters")[0].get("num_workers"), 0, 
                      description=f"The number of spark workers should be <b>0</b>.", hint=f"Found [[ACTUAL_VALUE]] workers.")

    # validate pipeline development mode, current channel, pipeline triggered mode
    suite.test_true(lambda: spec.get("development") != self.is_smoke_test(), 
                    description=f"The pipeline mode should be set to \"<b>Development</b>\".")
    suite.test(test_function=lambda: {spec.get("channel") is None or spec.get("channel").upper() == "CURRENT"}, 
               actual_value=spec.get("channel"), 
               description=f"The channel should be set to \"<b>Current</b>\".", hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\"")
    suite.test_false(lambda: spec.get("continuous"), 
                     description=f"Expected the Pipeline mode to be \"<b>Triggered</b>\".", 
                     hint=f"Found \"<b>Continuous</b>\".")

    # validate photon enabled
    # suite.test_true(lambda: spec.get("photon"), description=f"Photon should be enabled.")                     

    if display: suite.display_results()
    assert suite.passed, "One or more tests failed; please double check your work."  

None   

