import logging
import re
from typing import Any, Optional, Dict
from crm.pipedrive_client import PipedriveClient
from utils.text import normalize_text

logger = logging.getLogger(__name__)

BLACKLIST_TOKENS = {
    "blacklist", "blacklisted", "blocked", "lead_blocked", 
    "opt_out", "do_not_contact", "nao_enviar", "nao_contatar",
    "lead_trafego", "contato_indicacao_carol", "sem_interesse",
    "numero_errado", "contato_indicado"
}

def validate_contact_in_crm(contact_identifier: str) -> bool:
    """
    Validates if a contact exists in CRM and is not blacklisted.
    Uses Pipedrive Search API for efficiency.
    """
    identifier = normalize_text(contact_identifier).strip()
    if not identifier:
        logger.warning("CRM Validation Failed: Empty identifier")
        return False

    client = PipedriveClient()
    
    # 1. Search for person using search API
    items = client.find_person_by_term(identifier)
    
    if not items:
        logger.warning(f"CRM Validation Failed: '{identifier}' not found in Pipedrive.")
        return False

    # 2. Check if ANY match is valid (not blacklisted)
    for item in items:
        item_id = item.get('item', {}).get('id')
        if not item_id:
            continue
            
        person = client.get_person_details(item_id)
        if not person:
            continue
            
        if _is_blacklisted(person):
            logger.warning(f"CRM Validation Failed: Contact {item_id} is blacklisted.")
            continue # Try next match if any
            
        logger.info(f"CRM Validation Success: Found valid contact {item_id} for '{identifier}'")
        return True

    return False

def _is_blacklisted(person_data: Dict[str, Any]) -> bool:
    """
    Checks person data against blacklist tokens.
    """
    # Check simple fields
    to_check = [
        str(person_data.get('name', '')),
        str(person_data.get('label', '')), # Label can be ID or list
    ]
    
    # Check custom fields (often keys are hashes like '593...')
    # We iterate all values to be safe
    for key, value in person_data.items():
        if isinstance(value, str):
            to_check.append(value)
        elif isinstance(value, list): # For multi-select fields
             for v in value:
                 to_check.append(str(v))

    normalized_tokens = set()
    for text in to_check:
        normalized = normalize_text(text).lower()
        normalized_tokens.update(re.split(r'\W+', normalized))

    if BLACKLIST_TOKENS.intersection(normalized_tokens):
        return True
        
    return False
