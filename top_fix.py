import os
import sys
import time
import json
import re
import socket
import signal
import atexit
import logging
import threading
import subprocess
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import requests

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(ROOT_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

try:
    from logic.whatsapp_pitch_engine import WhatsAppPitchEngine
except ImportError:
    try:
        from whatsapp_pitch_engine import WhatsAppPitchEngine
    except ImportError:
        WhatsAppPitchEngine = None

from crm.pipedrive_client import PipedriveClient
from services.whatsapp_service import WhatsAppService
