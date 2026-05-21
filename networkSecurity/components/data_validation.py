from networkSecurity.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from networkSecurity.entity.config_entity import DataValidationConfig
from networkSecurity.exception.exception import NetworkSecurityException
from networkSecurity.logging.logger import logging
from scipy.stats import ks_2samp
import pandas as pd
import os,sys
