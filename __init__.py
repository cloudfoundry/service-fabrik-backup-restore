"""
Backup & Restore Python Library consumable by Cloud Foundry Services managed by the Service Fabrik
"""

__author__ = ["Rafael Franzke", "Holger Koser", "Vedran Lerenc", "Murali Suresh"]
__version__ = "1.2.2"

from .lib.config import parse_options
from .lib.clients.index import create_iaas_client
