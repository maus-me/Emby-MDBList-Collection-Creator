"""
This module handles the processing of MDBList collections.
It manages the creation, updating, and maintenance of Emby collections based on MDBList data.
"""
import configparser
import logging
import random
import time
from typing import Dict, List, Optional, Tuple, Any, Set

from src.connectors.mdblist import Mdblist
from src.date_parser import inside_period
from src.db import Db
from src.connectors.emby import Emby
from src.item_sorting import ItemSorting
from src.modules.collections_parse import load_config
from src.modules.config_parse import *
from src.refresher import Refresher
from src.utils import find_missing_entries_in_list
from src.utils import minutes_until_2100

logger = logging.getLogger(__name__)

# Global counters and collection tracking
newly_added = 0
newly_removed = 0
collection_ids_with_custom_sorting: List[str] = []
all_collections_ids: List[str] = []

# Initialize service connections
emby = Emby(EMBY_SERVER_URL, EMBY_USER_ID, EMBY_API_KEY)
mdblist = Mdblist(MDBLIST_API_KEY)
item_sorting = ItemSorting(emby)
refresher = Refresher(emby)
db_manager = Db()

# Load collections configuration
collections_parser = load_config()

def run() -> None:
    """
    Main execution function that runs the collection processing workflow.
    
    This function:
    1. Processes collections from hardcoded lists and/or MDBList
    2. Updates sort names for items in collections
    3. Refreshes metadata for recently added or premiered items
    4. Runs continuously in a loop
    
    Returns:
        None
    """
    global newly_added
    global newly_removed

    while True:
        try:
            # Process collections from different sources
            if DOWNLOAD_MANUALLY_ADDED_LISTS:
                process_hardcoded_lists()
            if DOWNLOAD_MY_MDBLIST_LISTS_AUTOMATICALLY:
                process_my_lists_on_mdblist()

            # Log summary of changes
            logger.info(f"SUMMARY: Added {newly_added} to collections and removed {newly_removed}")
            newly_added = 0
            newly_removed = 0

            # Update sort names for items in collections with custom sorting
            if collection_ids_with_custom_sorting:
                logger.info("Setting sort names for new items in collections")
                for collection_id in collection_ids_with_custom_sorting:
                    item_sorting.process_collection(collection_id)

                logger.info("Reverting sort names that are no longer in collections, fetching items:")

            # Reset sort names for items no longer in collections with custom sorting
            item_sorting.reset_items_not_in_custom_sort_categories()

            # Refresh metadata for recently added or premiered items
            if REFRESH_ITEMS:
                logger.info(
                    f"Refreshing metadata for items that were added within {REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_ADDED} days "
                    f"AND premiered within {REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_PREMIERED} days."
                )
                
                for collection_id in all_collections_ids:
                    refresher.process_collection(
                        collection_id,
                        REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_ADDED,
                        REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_PREMIERED,
                    )
        except Exception as e:
            logger.error(f"Error in main processing loop: {e}")
            time.sleep(60)  # Wait a minute before retrying

def set_poster(collection_id: str, collection_name: str, poster_path: Optional[str] = None) -> bool:
    """
    Sets the poster for a collection if it hasn't been set before or has changed.
    
    This function checks if the poster has already been set to the specified path
    by querying the database. If not, it attempts to set the poster and updates
    the database record on success.

    Args:
        collection_id: The ID of the collection
        collection_name: The name of the collection (used for logging)
        poster_path: The path or URL to the poster image

    Returns:
        bool: True if poster was set successfully or was already set, False otherwise
    """
    # Skip if no poster path provided
    if not poster_path:
        return True
    
    # Check if poster is already set to this path
    current_poster = db_manager.get_config_for_section(collection_id, "poster_path")
    if poster_path == current_poster:
        logger.debug(f"Poster for '{collection_name}' is already set to the specified path.")
        return True

    # Attempt to set the poster
    try:
        if emby.set_image(collection_id, poster_path):
            db_manager.set_config_for_section(collection_id, "poster_path", poster_path)
            logger.info(f"Poster for '{collection_name}' has been set successfully.")
            return True
        else:
            logger.warning(f"Failed to set poster for '{collection_name}'.")
            return False
    except Exception as e:
        logger.error(f"Error setting poster for '{collection_name}': {e}")
        return False


def process_hardcoded_lists() -> None:
    """
    Process collections defined in the collections configuration file.
    
    This function reads the collections configuration, parses each section into a
    dictionary of collection properties, and passes them to process_list().
    
    Returns:
        None
    """
    collections: List[Dict[str, Any]] = []
    
    # Parse each section in the collections configuration
    for section in collections_parser.sections():
        try:
            # Create a dictionary of collection properties
            collection_config = {
                "name": section,
                "id": collections_parser.get(section, "id", fallback=None),
                "source": collections_parser.get(section, "source", fallback=""),
                "poster": collections_parser.get(section, "poster", fallback=None),
                "frequency": collections_parser.get(section, "frequency", fallback=100),
                "mdblist_name": collections_parser.get(section, "list_name", fallback=None),
                "user_name": collections_parser.get(section, "user_name", fallback=None),
                "update_items_sort_names": collections_parser.getboolean(
                    section, "update_items_sort_names", fallback=False
                ),
                "collection_sort_name": collections_parser.get(
                    section, "collection_sort_name", fallback=None
                ),
                "collection_sort_prefix": collections_parser.get(
                    section, "collection_sort_prefix", fallback=None
                ),
                "collection_sort_date": collections_parser.getboolean(
                    section, "collection_sort_date", fallback=False
                ),
                "overwrite_description": collections_parser.get(
                    section, "description", fallback=None
                ),
                "active_between": collections_parser.get(
                    section, "active_between", fallback=None
                )
            }
            
            collections.append(collection_config)
            logger.debug(f"Parsed collection config for '{section}'")
            
        except configparser.NoOptionError as e:
            logger.error(f"Error in config file, section: {section}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error parsing section '{section}': {e}")

    # Process each collection
    logger.info(f"Processing {len(collections)} hardcoded collections")
    for mdblist_list in collections:
        try:
            process_list(mdblist_list)
        except Exception as e:
            logger.error(f"Error processing list '{mdblist_list.get('name', 'unknown')}': {e}")

def process_my_lists_on_mdblist() -> None:
    """
    Process all lists owned by the authenticated MDBList user.
    
    This function fetches all lists from the user's MDBList account and
    processes each one using the process_list() function.
    
    Returns:
        None
    """
    try:
        # Fetch lists from MDBList API
        my_lists = mdblist.get_my_lists()
        
        # Check if any lists were returned
        if not my_lists:
            logger.error("No lists returned from MDBList API. Will not process any lists.")
            return
            
        logger.info(f"Processing {len(my_lists)} lists from MDBList account")
        
        # Process each list
        for mdblist_list in my_lists:
            try:
                list_name = mdblist_list.get('name', 'Unknown list')
                logger.debug(f"Processing MDBList list: {list_name}")
                process_list(mdblist_list)
            except Exception as e:
                logger.error(f"Error processing MDBList list '{list_name}': {e}")
                
    except Exception as e:
        logger.error(f"Error fetching lists from MDBList: {e}")


def process_list(mdblist_list: Dict[str, Any]) -> None:
    """
    Process a single MDBList collection and update the corresponding Emby collection.
    
    This function:
    1. Extracts collection parameters from the mdblist_list dictionary
    2. Checks if the collection is active (for seasonal collections)
    3. Fetches items from MDBList based on list ID, name, or source URL
    4. Compares with existing items in the Emby collection
    5. Adds missing items and removes items no longer in the list
    6. Sets collection properties (poster, sort name, description)
    
    Args:
        mdblist_list: Dictionary containing collection parameters
        
    Returns:
        None
    """
    global newly_added
    global newly_removed
    
    # Extract collection parameters
    collection_name = mdblist_list["name"]
    frequency = int(mdblist_list.get("frequency", 100))
    list_id = mdblist_list.get("id", None)
    source = mdblist_list.get("source", None)
    poster = mdblist_list.get("poster", None)
    mdblist_name = mdblist_list.get("mdblist_name", None)
    user_name = mdblist_list.get("user_name", None)
    update_collection_items_sort_names = mdblist_list.get(
        "update_items_sort_names", UPDATE_ITEMS_SORT_NAMES_DEFAULT_VALUE
    )
    collection_sort_name = mdblist_list.get("collection_sort_name", None)
    collection_sort_prefix = mdblist_list.get("collection_sort_prefix", None)
    collection_sort_date = mdblist_list.get("collection_sort_date", False)
    description = mdblist_list.get("description", None)  # Description from mdblist
    overwrite_description = mdblist_list.get("overwrite_description", None)  # From cfg
    active_period_str = mdblist_list.get("active_between", None)

    # Get collection ID if it exists
    collection_id = emby.get_collection_id(collection_name)
    
    # Handle seasonal collections
    if active_period_str:
        if not inside_period(active_period_str):
            # Collection exists but is not in active period - remove all items
            if collection_id is None:
                logger.info(f"Seasonal collection '{collection_name}' does not exist. Will not process.")
                return

            try:
                # Get all items in the collection
                all_items_in_collection = emby.get_items_in_collection(collection_id, ["Id"])
                item_ids = (
                    [item["Id"] for item in all_items_in_collection]
                    if all_items_in_collection is not None
                    else []
                )

                # Remove all items from the collection
                if item_ids:
                    newly_removed += emby.delete_from_collection(collection_name, item_ids)
                    logger.info(f"Collection '{collection_name}' is not active. Removed {newly_removed} items.")
                else:
                    logger.info(f"Collection '{collection_name}' is not active. No items to remove.")
            except Exception as e:
                logger.error(f"Error handling inactive seasonal collection '{collection_name}': {e}")
            return

    # If collection doesn't exist, always process it
    if collection_id is None:
        logger.info(f"Collection '{collection_name}' does not exist. Will create it.")
        frequency = 100  # If collection doesn't exist, download every time

    # Apply frequency filter (random chance to skip processing)
    if random.randint(0, 100) > frequency:
        logger.info(f"Skipping mdblist '{collection_name}' since frequency is {frequency}")
        return

    # Fetch items from MDBList
    mdblist_imdb_ids: List[str] = []
    mdblist_mediatypes: List[str] = []
    
    try:
        # Get items based on list ID
        if list_id is not None:
            mdblist_imdb_ids, mdblist_mediatypes = mdblist.get_list(list_id)
        # Get items based on list name and user name
        elif mdblist_name is not None and user_name is not None:
            found_list_id = mdblist.find_list_id_by_name_and_user(mdblist_name, user_name)
            if found_list_id is None:
                logger.error(f"List '{mdblist_name}' by '{user_name}' not found. Skipping.")
                return
            mdblist_imdb_ids, mdblist_mediatypes = mdblist.get_list(found_list_id)
        # Get items based on source URL(s)
        elif source is not None:
            # Clean up source string and split multiple URLs
            source = source.replace(" ", "")
            sources = source.split(",http")
            # Add http back to all but the first source
            sources = [sources[0]] + [f"http{url}" for url in sources[1:]]
            
            # Process each source URL
            for url in sources:
                imdb_ids, mediatypes = mdblist.get_list_using_url(url.strip())
                if imdb_ids:
                    mdblist_imdb_ids.extend(imdb_ids)
                    mdblist_mediatypes.extend(mediatypes)
        else:
            logger.error(f"Must provide either id, list_name+user_name, or source for '{collection_name}'.")
            return
    except Exception as e:
        logger.error(f"Error fetching items for '{collection_name}': {e}")
        return

    # Validate fetched items
    if mdblist_imdb_ids is None:
        logger.error(f"No items returned for '{collection_name}'. Will not process this list.")
        return

    if len(mdblist_imdb_ids) == 0:
        logger.error(
            f"No items in mdblist '{collection_name}'. Will not process this list. Perhaps you need to wait for it to populate?"
        )
        return

    # Remove duplicates
    mdblist_imdb_ids = list(set(mdblist_imdb_ids))
    logger.info(f"Processing '{collection_name}'. List has {len(mdblist_imdb_ids)} IMDB IDs")
    
    # Refresh collection ID (in case it was created in another thread)
    collection_id = emby.get_collection_id(collection_name)

    # Determine which items to add and remove
    remove_emby_ids: List[str] = []
    
    if collection_id is None:
        # New collection - all items need to be added
        missing_imdb_ids = mdblist_imdb_ids
    else:
        # Existing collection - compare with current items
        try:
            collection_items = emby.get_items_in_collection(collection_id, ["ProviderIds"])
            
            # Extract IMDB IDs from collection items
            collection_imdb_ids = [item["Imdb"] for item in collection_items]
            
            # Find items to add (in MDBList but not in collection)
            missing_imdb_ids = find_missing_entries_in_list(collection_imdb_ids, mdblist_imdb_ids)
            
            # Find items to remove (in collection but not in MDBList)
            for item in collection_items:
                if item["Imdb"] not in mdblist_imdb_ids:
                    remove_emby_ids.append(item["Id"])
        except Exception as e:
            logger.error(f"Error comparing collection items for '{collection_name}': {e}")
            return

    # Convert IMDB IDs to Emby item IDs
    add_emby_ids = emby.get_items_with_imdb_id(missing_imdb_ids, mdblist_mediatypes)
    logger.info(f"Found {len(add_emby_ids)} items to add and {len(remove_emby_ids)} items to remove")

    # Create collection if it doesn't exist
    if collection_id is None:
        if not add_emby_ids:
            logger.error(f"No items to put in mdblist '{collection_name}'.")
            return
            
        # Create the collection with the first item (required by Emby)
        try:
            collection_id = emby.create_collection(collection_name, [add_emby_ids[0]])
            add_emby_ids.pop(0)
            logger.info(f"Created new collection '{collection_name}'")
        except Exception as e:
            logger.error(f"Error creating collection '{collection_name}': {e}")
            return

    # Track collection for later processing
    if collection_id not in all_collections_ids:
        all_collections_ids.append(collection_id)

    if update_collection_items_sort_names and collection_id not in collection_ids_with_custom_sorting:
        collection_ids_with_custom_sorting.append(collection_id)

    # Add and remove items
    try:
        items_added = emby.add_to_collection(collection_name, add_emby_ids)
        newly_added += items_added
        
        items_removed = emby.delete_from_collection(collection_name, remove_emby_ids)
        newly_removed += items_removed
        
        logger.info(f"Added {items_added} items and removed {items_removed} items from '{collection_name}'")
    except Exception as e:
        logger.error(f"Error updating items in collection '{collection_name}': {e}")

    # Set collection poster
    set_poster(collection_id, collection_name, poster)

    # Handle collection sort name
    try:
        sort_title_update = False
        
        # Use collection name as default sort name if not specified
        if collection_sort_name is None:
            collection_sort_name = collection_name

        # Remove unwanted words from the start of the collection name
        # Mirrors the Emby sort name removal defaults found in system.xml
        unwanted_words = ["the", "a", "an", "das", "der", "el", "la"]
        for word in unwanted_words:
            word_with_space = f'{word} '
            if collection_sort_name.lower().startswith(word_with_space):
                collection_sort_name = collection_sort_name[len(word_with_space):]

        # Apply date-based sorting if enabled and collection has changed
        if collection_sort_date and (items_added > 0 or items_removed > 0):
            collection_sort_name = f"!{minutes_until_2100()} {collection_sort_name}"
            sort_title_update = True
            logger.info(f"Updated sort name for '{collection_name}' to '{collection_sort_name}' (date-based)")
        # Apply prefix-based sorting if specified
        elif collection_sort_prefix is not None:
            collection_sort_name = f"{collection_sort_prefix} {collection_sort_name}"
            sort_title_update = True
            logger.info(f"Updated sort name for '{collection_name}' to '{collection_sort_name}' (prefix-based)")

        # Update sort name if needed
        if sort_title_update:
            emby.set_item_property(collection_id, "ForcedSortName", collection_sort_name)
        else:
            logger.debug(f"Collection '{collection_name}' will inherit the name for sorting.")
    except Exception as e:
        logger.error(f"Error setting sort name for '{collection_name}': {e}")

    # Handle collection description
    try:
        # Use MDBList description if enabled and available
        if (USE_MDB_COLLECTION_DESCRIPTION and 
                description is not None and description != "" and
                overwrite_description is None):
            # Strip any leading and trailing quotes from the description
            description = strip_quotes(description)
            emby.set_item_property(collection_id, "Overview", description)
            logger.debug(f"Set description from MDBList for '{collection_name}'")
        # Use overwrite description if specified
        elif overwrite_description is not None and overwrite_description != "":
            # Strip any leading and trailing quotes from the description
            overwrite_description = strip_quotes(overwrite_description)
            emby.set_item_property(collection_id, "Overview", overwrite_description)
            logger.debug(f"Set custom description for '{collection_name}'")
    except Exception as e:
        logger.error(f"Error setting description for '{collection_name}': {e}")


def strip_quotes(text: Optional[str]) -> Optional[str]:
    """
    Strip leading and trailing quotes from a string.
    
    This function removes matching single or double quotes from the beginning
    and end of a string if they exist.
    
    Args:
        text: The string to process
        
    Returns:
        The string with leading and trailing quotes removed, or the original
        string if no matching quotes are found. Returns None if input is None.
    """
    if text is None or text == "":
        return text

    # Check for matching quotes at beginning and end
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
        
    return text