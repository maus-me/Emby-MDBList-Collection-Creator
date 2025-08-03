"""
Configuration Parser Module

This module handles loading and parsing of configuration settings from the collections config files.
It provides access to configuration values through exported variables and functions.
"""
import configparser
import logging
import random
import time

from src.connectors.mdblist import Mdblist
from src.date_parser import inside_period
from src.db import Db
from src.connectors.emby import Emby
from src.item_sorting import ItemSorting
from src.modules.collections_parse import load_config
from src.modules.config_parse import *
from src.modules.config_parse import config_parser
from src.refresher import Refresher
from src.utils import find_missing_entries_in_list
from src.utils import minutes_until_2100

logger = logging.getLogger(__name__)

newly_added = 0
newly_removed = 0
collection_ids_with_custom_sorting = []
all_collections_ids = []

emby = Emby(EMBY_SERVER_URL, EMBY_USER_ID, EMBY_API_KEY)
mdblist = Mdblist(MDBLIST_API_KEY)
item_sorting = ItemSorting(emby)
refresher = Refresher(emby)
db_manager = Db()

# Load configuration
collections_parser = load_config()

def run():
    global newly_added
    global newly_removed
    iterations = 0

    while True:
        if DOWNLOAD_MANUALLY_ADDED_LISTS:
            process_hardcoded_lists()
        if DOWNLOAD_MY_MDBLIST_LISTS_AUTOMATICALLY:
            process_my_lists_on_mdblist()

        logger.info(
            f"SUMMARY: Added {newly_added} to collections and removed {newly_removed}\n\n"
        )
        newly_added = 0
        newly_removed = 0

        if len(collection_ids_with_custom_sorting) > 0:
            logger.info("Setting sort names for new items in collections")
            for collection_id in collection_ids_with_custom_sorting:
                item_sorting.process_collection(collection_id)

            logger.info(
                "\n\nReverting sort names that are no longer in collections, fetching items:"
            )

        item_sorting.reset_items_not_in_custom_sort_categories()

        if REFRESH_ITEMS:
            logger.info(
                f"Refreshing metadata for items that were added within {REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_ADDED} days AND premiered within {REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_PREMIERED} days.\n"
            )

        for collection_id in all_collections_ids:
            if REFRESH_ITEMS:
                refresher.process_collection(
                    collection_id,
                    REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_ADDED,
                    REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_PREMIERED,
                )

        if HOURS_BETWEEN_REFRESH == 0:
            break

        logger.info(f"Waiting {HOURS_BETWEEN_REFRESH} hours for next refresh.\n\n")
        time.sleep(HOURS_BETWEEN_REFRESH * 3600)
        iterations += 1

def set_poster(collection_id, collection_name, poster_path=None):
    """
    Sets the poster for a collection. Will not upload if temp config file
    shows that it has been uploaded before.

    Args:
        collection_id (str): The ID of the collection.
        collection_name (str): The name of the collection. Only used for logger.
        poster_path (str): The path or URL to the new poster image.

    Returns:
        None
    """

    if poster_path is None:
        return

    if poster_path == db_manager.get_config_for_section(collection_id, "poster_path"):
        logger.info(f"Poster for {collection_name} is already set to the specified path.")
        return

    if emby.set_image(collection_id, poster_path):
        db_manager.set_config_for_section(collection_id, "poster_path", poster_path)
        logger.info(f"Poster for {collection_name} has been set successfully.")
    else:
        logger.info(f"Failed to set poster for {collection_name}.")


def process_hardcoded_lists():

    collections = []
    for section in collections_parser.sections():
        try:
            collections.append(
                {
                    "name": section,
                    "id": collections_parser.get(section, "id", fallback=None),
                    "source": collections_parser.get(section, "source", fallback=""),
                    "poster": collections_parser.get(section, "poster", fallback=None),
                    "frequency": collections_parser.get(section, "frequency", fallback=100),
                    "mdblist_name": collections_parser.get(
                        section, "list_name", fallback=None
                    ),
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
                    "collection_sort_date": collections_parser.get(
                        section, "collection_sort_date", fallback=False
                    ),
                    "overwrite_description": collections_parser.get(
                        section, "description", fallback=None
                    ),
                }
            )
        except configparser.NoOptionError as e:
            logger.error(f"Error in config file, section: {section}: {e}")

    for mdblist_list in collections:
        process_list(mdblist_list)

def process_my_lists_on_mdblist():
    my_lists = mdblist.get_my_lists()
    if len(my_lists) == 0:
        logger.error("No lists returned from MDBList API. Will not process any lists.")
        return

    for mdblist_list in my_lists:
        process_list(mdblist_list)


def process_list(mdblist_list: dict):
    global newly_added
    global newly_removed
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

    collection_id = emby.get_collection_id(collection_name)

    active_period_str = config_parser.get(
        collection_name, "active_between", fallback=None
    )

    if active_period_str:
        if not inside_period(active_period_str):
            # Check if collection even exists
            if collection_id is None:
                logger.info(f"Seasonal collection {collection_name} does not exist. Will not process.")
                return

            all_items_in_collection = emby.get_items_in_collection(
                collection_id, ["Id"]
            )
            item_ids = (
                [item["Id"] for item in all_items_in_collection]
                if all_items_in_collection is not None
                else []
            )

            newly_removed += emby.delete_from_collection(collection_name, item_ids)

            if newly_removed > 0:
                logger.info(f"Collection {collection_name} is not active. Removed all items.")
            else:
                logger.info(f"Collection {collection_name} is not active. No items to remove.")
            return

    if collection_id is None:
        logger.info(f"Collection {collection_name} does not exist. Will create it.")
        frequency = 100  # If collection doesn't exist, download every time


    if random.randint(0, 100) > frequency:
        logger.info(f"Skipping mdblist {collection_name} since frequency is {frequency}")
        return

    mdblist_imdb_ids = []
    mdblist_mediatypes = []
    if list_id is not None:
        mdblist_imdb_ids, mdblist_mediatypes = mdblist.get_list(list_id)
    elif mdblist_name is not None and user_name is not None:
        found_list_id = mdblist.find_list_id_by_name_and_user(mdblist_name, user_name)
        if found_list_id is None:
            logger.error(f"List {mdblist_name} by {user_name} not found. Skipping.")
            return
        mdblist_imdb_ids, mdblist_mediatypes = mdblist.get_list(found_list_id)
    elif source is not None:
        source = source.replace(" ", "")
        sources = source.split(",http")
        # Add http back to all but the first source
        sources = [sources[0]] + [f"http{url}" for url in sources[1:]]
        for url in sources:
            imdb_ids, mediatypes = mdblist.get_list_using_url(url.strip())
            mdblist_imdb_ids.extend(imdb_ids)
            mdblist_mediatypes.extend(mediatypes)
    else:
        logger.error(f"Must provide either id or source for {collection_name}.")
        return

    if mdblist_imdb_ids is None:
        logger.error(f"No items in {collection_name}. Will not process this list.")
        return

    remove_emby_ids = []

    if len(mdblist_imdb_ids) == 0:
        logger.error(
            f"No items in mdblist {collection_name}. Will not process this list. Perhaps you need to wait for it to populate?"
        )
        return

    mdblist_imdb_ids = list(set(mdblist_imdb_ids))  # Remove duplicates
    logger.info(f"Processing {collection_name}. List has {len(mdblist_imdb_ids)} IMDB IDs")
    collection_id = emby.get_collection_id(collection_name)

    if collection_id is None:
        missing_imdb_ids = mdblist_imdb_ids
    else:
        try:
            collection_items = emby.get_items_in_collection(
                collection_id, ["ProviderIds"]
            )
        except Exception as e:
            logger.error(f"Exception getting items in collection: {e}")
            return

        collection_imdb_ids = [item["Imdb"] for item in collection_items]
        missing_imdb_ids = find_missing_entries_in_list(
            collection_imdb_ids, mdblist_imdb_ids
        )

        for item in collection_items:
            if item["Imdb"] not in mdblist_imdb_ids:
                remove_emby_ids.append(item["Id"])

    # Need Emby Item Ids instead of IMDB IDs to add to collection
    add_emby_ids = emby.get_items_with_imdb_id(missing_imdb_ids, mdblist_mediatypes)

    logger.info(f"Added {len(add_emby_ids)} new items and removed {len(remove_emby_ids)}")

    if collection_id is None:
        if len(add_emby_ids) == 0:
            logger.error(f"No items to put in mdblist {collection_name}.")
            return
        # Create the collection with the first item since you have to create with an item
        collection_id = emby.create_collection(collection_name, [add_emby_ids[0]])
        add_emby_ids.pop(0)

    if collection_id not in all_collections_ids:
        all_collections_ids.append(collection_id)

    if update_collection_items_sort_names:
        collection_ids_with_custom_sorting.append(collection_id)

    items_added = emby.add_to_collection(collection_name, add_emby_ids)
    newly_added += items_added
    newly_removed += emby.delete_from_collection(collection_name, remove_emby_ids)

    set_poster(collection_id, collection_name, poster)

    sort_title_update = False

    # Formatted as: '{Prefix} {Time} {Collection Name}'

    # If collection_sort_name is None, it will be set to the collection name
    if collection_sort_name is None:
        collection_sort_name = collection_name

    # Remove unwanted words from the start of the collection name
    # Mirrors the Emby sort name removal defaults found in system.xml
    unwanted_words = ["the", "a", "an", "das", "der", "el", "la"]
    for word in unwanted_words:
        word_with_space = f'{word} '
        if collection_sort_name.lower().startswith(word_with_space):
            collection_sort_name = collection_sort_name[len(word_with_space):]

    if collection_sort_date is True and (items_added > 0 or newly_removed > 0):
        collection_sort_name = f"!{minutes_until_2100()} {collection_sort_name}"
        sort_title_update = True
        logger.info(f"Updated sort name for {collection_name} to {collection_sort_name}")

    elif collection_sort_prefix is not None:
        collection_sort_name = f"{collection_sort_prefix} {collection_sort_name}"
        sort_title_update = True
        logger.info(f"Updated sort name for {collection_name} to {collection_sort_name}")

    # No need to update the sort name if the collection doesn't use a custom sort name.  Will inherit the name.
    if sort_title_update:
        emby.set_item_property(collection_id, "ForcedSortName", collection_sort_name)
    else:
        logger.info(f"Collection {collection_name} will inherit the name.")



    if (USE_MDB_COLLECTION_DESCRIPTION is True
            and bool(description)
            and overwrite_description is None
    ):
        # Strip any leading and trailing quotes from the description
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        # Also handle single quotes if needed
        elif description.startswith("'") and description.endswith("'"):
            description = description[1:-1]

        emby.set_item_property(collection_id, "Overview", description)
    elif overwrite_description is not None:
        # Strip any leading and trailing quotes from the description
        if overwrite_description.startswith('"') and overwrite_description.endswith('"'):
            overwrite_description = overwrite_description[1:-1]
        # Also handle single quotes if needed
        elif overwrite_description.startswith("'") and overwrite_description.endswith("'"):
            overwrite_description = overwrite_description[1:-1]

        emby.set_item_property(collection_id, "Overview", overwrite_description)
