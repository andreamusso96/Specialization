from DataPreprocessing.OMSAData.Pipeline import Pipeline as OMSAPipeline
from DataPreprocessing.ONETData.Pipeline import Pipeline as ONETPipeline


def run_pipelines():
    onet_pipeline = ONETPipeline()
    onet_pipeline.run()
    omsa_pipeline = OMSAPipeline()
    omsa_pipeline.run()


if __name__ == '__main__':
    run_pipelines()